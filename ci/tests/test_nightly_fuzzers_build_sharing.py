"""
Regression tests for NightlyFuzzers sharing MasterCI's ARM release build.

NightlyFuzzers needs the release binary to generate the fuzzer dictionary, and
gets it by reusing the build MasterCI already ran for the same commit. Three
things have to line up for that, and all three were wrong:

  - The job digest covers `command` and the `provides` artifact configs
    (ci/praktika/digest.py drops only requires/enable_commit_status/
    allow_failure/force_success/digest_config). MasterCI takes
    release_build_jobs_with_examples, which appends --build-examples and
    CLICKHOUSE_EXAMPLES to the ARM release job, so a workflow taking the plain
    release_build_jobs variant hashes differently and rebuilds from scratch.

  - The long-retention tags are part of those artifact configs, and the upload
    path is keyed by branch and commit. An untagged upload therefore both misses
    the cache and replaces MasterCI's long-retention binary with a
    default-retention one.

  - `run_after` is hashed too, and mangle appends the docker job names to it, so
    the two workflows must inject the same docker jobs. MasterCI merges a
    multiplatform manifest; a workflow that does not gets a shorter run_after
    and a different key, however well the rest agrees.

The digests are therefore compared post-mangle: the pre-mangle objects miss
that last difference and report a cache hit the runtime does not have.
"""

import dataclasses
import hashlib
import importlib
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.defs import (  # noqa: E402
    BINARIES_WITH_LONG_RETENTION,
    ArtifactConfigs,
    ArtifactNames,
    with_long_retention_tags,
)
from ci.defs.job_configs import JobConfigs  # noqa: E402
from ci.praktika import Workflow  # noqa: E402
from ci.praktika.native_jobs import _publish_latest_docker_manifest  # noqa: E402
from ci.workflows.master import workflow as master_workflow  # noqa: E402
from ci.workflows.nightly_fuzzers import workflow as nightly_workflow  # noqa: E402
from ci.workflows.nightly_jepsen import workflow as jepsen_workflow  # noqa: E402
from ci.workflows.nightly_sqlancer import workflow as sqlancer_workflow  # noqa: E402
from ci.workflows.release_branches import (  # noqa: E402
    workflow as release_workflow,
)
from ci.workflows.weekly_fuzzers_corpus import (  # noqa: E402
    workflow as corpus_workflow,
)

_SHARED_BUILD = "Build (arm_release)"

# ci/praktika/digest.py's drop list, mirrored so a change there surfaces here.
_DROPPED_FROM_DIGEST = [
    "requires",
    "enable_commit_status",
    "allow_failure",
    "force_success",
    "digest_config",
]


def _tags(artifact):
    # add_tags stores tags in `ext`, not in an attribute.
    return artifact.ext.get("tags")


def _job(workflow, name):
    for job in workflow.jobs:
        if job.name == name:
            return job
    raise AssertionError(f"{workflow.name} has no job {name!r}")


def _mangled(name):
    """The workflow as praktika will run it.

    The imported module-level object is the pre-mangle one: mangle injects the
    docker and native jobs and appends them to every other job's `run_after`,
    so a digest computed from the import misses differences that decide the
    real cache key.
    """
    from ci.praktika.mangle import _get_workflows

    for workflow in _get_workflows():
        if workflow.name == name:
            return workflow
    raise AssertionError(f"no workflow named {name!r}")


def _config_digest(job, workflow):
    """The job-config half of the digest, as hook_cache.py feeds it.

    Reproduced rather than imported because calc_job_digest also hashes the
    include_paths file contents and the docker digests, which are irrelevant
    here and would make the comparison depend on the working tree.
    """
    artifact_configs = {a.name: a for a in workflow.artifacts}
    job_dict = dataclasses.asdict(job)
    filtered = {k: v for k, v in job_dict.items() if k not in _DROPPED_FROM_DIGEST}
    filtered["provides"] = [
        dataclasses.asdict(artifact_configs[a])
        for a in job.provides
        if a in artifact_configs
    ]
    return hashlib.md5(json.dumps(filtered, sort_keys=True).encode()).hexdigest()[:4]


class TestBuildIsSharedWithMasterCI:
    def test_digest_matches_master(self):
        # The workflow comment claims a cache hit; this is that claim, computed
        # from the mangled workflows because that is what praktika hashes.
        nightly, master = _mangled("NightlyFuzzers"), _mangled("MasterCI")
        assert _config_digest(_job(nightly, _SHARED_BUILD), nightly) == _config_digest(
            _job(master, _SHARED_BUILD), master
        )

    def test_the_pre_mangle_comparison_is_not_what_decides_the_cache_key(self):
        # Negative control for the test above. Mangle appends the docker job
        # names to run_after, and run_after is hashed, so comparing the
        # imported objects can report a match the runtime does not have.
        nightly, master = _mangled("NightlyFuzzers"), _mangled("MasterCI")
        assert _job(nightly, _SHARED_BUILD).run_after, (
            "mangle did not populate run_after, so this control cannot "
            "distinguish the pre- and post-mangle comparisons"
        )
        assert (
            _job(nightly, _SHARED_BUILD).run_after
            == _job(master, _SHARED_BUILD).run_after
        ), "the docker job graphs differ, so the release builds cannot share a cache entry"

    def test_cache_sharing_workflows_agree_on_the_manifest_behaviour(self):
        # The merge job's digest does not cover set_latest_for_docker_merged_
        # manifest, so aligning the graph to share a cache entry also means
        # whichever workflow runs the job first decides whether `latest` is
        # tagged. Enumerated over the shared job so a third sharer is covered.
        sharing = [
            w
            for w in (_mangled("MasterCI"), _mangled("NightlyFuzzers"))
            if any(j.name == _SHARED_BUILD for j in w.jobs)
        ]
        assert len(sharing) == 2, [w.name for w in sharing]
        assert (
            len({w.set_latest_for_docker_merged_manifest for w in sharing}) == 1
        ), "workflows sharing the manifest job must agree on tagging `latest`"

    def test_plain_release_variant_would_not_match(self):
        # Mutation arm: the variant NightlyFuzzers used to take. Without this,
        # the equality above could hold for reasons unrelated to the fix.
        stale_job = next(
            j for j in JobConfigs.release_build_jobs if "arm_release" in j.name
        )
        stale_workflow = dataclasses.replace(
            nightly_workflow,
            artifacts=[
                *ArtifactConfigs.clickhouse_binaries,
                *ArtifactConfigs.clickhouse_debians,
                *ArtifactConfigs.clickhouse_rpms,
                *ArtifactConfigs.clickhouse_tgzs,
            ],
        )
        assert _config_digest(stale_job, stale_workflow) != _config_digest(
            _job(master_workflow, _SHARED_BUILD), master_workflow
        )

    def test_shared_job_takes_the_with_examples_variant(self):
        job = _job(nightly_workflow, _SHARED_BUILD)
        assert "--build-examples" in job.command
        assert "CLICKHOUSE_EXAMPLES" in job.provides

    def test_examples_artifact_is_declared(self):
        # The job provides it, so the workflow has to declare it or the upload
        # has nowhere to go.
        assert "CLICKHOUSE_EXAMPLES" in {a.name for a in nightly_workflow.artifacts}


class TestLatestTagIsBaseBranchOnly:
    """`latest` is a mutable alias every consumer of the images resolves, and the
    generated workflow is dispatchable on any ref, so which branch a run is on
    decides whether it may move the alias.
    """

    def test_the_declared_branches_still_publish(self):
        # Enumerated over the flag rather than listed, so a workflow that starts
        # tagging `latest` later is covered here too.
        holders = [
            w
            for w in (master_workflow, nightly_workflow)
            if w.set_latest_for_docker_merged_manifest
        ]
        assert holders, "no workflow tags `latest`: the arms below are vacuous"
        for workflow in holders:
            assert workflow.branches, workflow.name
            for branch in workflow.branches:
                assert _publish_latest_docker_manifest(workflow, branch), workflow.name

    def test_a_dispatch_from_another_branch_does_not(self):
        # Same workflow and same flag as the arm above, so the branch is the only
        # difference between them.
        assert nightly_workflow.set_latest_for_docker_merged_manifest
        assert not _publish_latest_docker_manifest(
            nightly_workflow, "groeneai/reland-fuzzer-dict-from-binary"
        )

    def test_a_workflow_that_does_not_tag_latest_never_publishes(self):
        assert not sqlancer_workflow.set_latest_for_docker_merged_manifest
        for branch in (*sqlancer_workflow.branches, "some/other-branch"):
            assert not _publish_latest_docker_manifest(sqlancer_workflow, branch)

    def test_every_workflow_in_the_tree_that_tags_latest_declares_a_branch(self):
        # A workflow with no branches cannot name its base branch, so the gate
        # falls back to publishing; that fallback has to stay unreachable.
        holders = []
        directory = os.path.join(os.path.dirname(__file__), "..", "workflows")
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".py") or name == "__init__.py":
                continue
            module = importlib.import_module(f"ci.workflows.{name[:-3]}")
            for workflow in getattr(module, "WORKFLOWS", []):
                if workflow.set_latest_for_docker_merged_manifest:
                    holders.append((workflow.name, list(workflow.branches)))
        assert holders, "no workflow tags `latest`: this arm is vacuous"
        assert [name for name, branches in holders if not branches] == []


class TestConsumerRequiresTheReleaseBinary:
    """The producer side above is only half of the wiring.

    `libFuzzer tests` generates the dictionary by running the release binary, so
    it has to declare that artifact. Without the edge the job starts with no
    binary to run and the sharing asserted above is pointless.
    """

    _CONSUMER = "libFuzzer tests"

    def test_job_config_requires_the_release_binary(self):
        assert ArtifactNames.CH_ARM_RELEASE in JobConfigs.libfuzzer_job.requires

    def test_the_workflow_job_requires_it_too(self):
        # The workflow takes the shared config, but a workflow is free to
        # substitute a job, so assert the instance the workflow will run.
        assert (
            ArtifactNames.CH_ARM_RELEASE
            in _job(nightly_workflow, self._CONSUMER).requires
        )

    def test_the_workflow_produces_what_the_consumer_requires(self):
        # An edge naming an artifact no job in this workflow provides would
        # never resolve.
        provided = {a for job in nightly_workflow.jobs for a in job.provides}
        required = set(_job(nightly_workflow, self._CONSUMER).requires)
        assert ArtifactNames.CH_ARM_RELEASE in provided
        assert required <= provided, sorted(required - provided)

    def test_dictionary_inputs_are_in_the_consumer_digest(self):
        # The job runs update_dict.sh against the curated dictionary, and
        # update_dict.sh in turn runs generate_source_dict.sh for the
        # source-vs-binary coverage check, so a change to any of the three has
        # to re-run it rather than take a cache hit.
        include_paths = JobConfigs.libfuzzer_job.digest_config.include_paths
        missing = [
            path
            for path in (
                "./tests/fuzz/update_dict.sh",
                "./tests/fuzz/generate_source_dict.sh",
                "./tests/fuzz/dictionaries/old.dict",
            )
            if path not in include_paths
        ]
        assert missing == [], missing


class TestOnlyTheGeneratingJobNeedsTheBinary:
    """`libfuzzer_test_check.py` also backs the weekly corpus-minimization job.

    A minimization run replays an existing corpus, which libFuzzer takes no
    dictionary for, so that job requires no release binary and its workflow
    provides none. Generating the dictionary there would assert on a binary that
    is not staged.
    """

    def _script_jobs(self):
        # `ci/defs` imports praktika as `praktika`, not `ci.praktika`, so its job
        # objects are instances of a different class object than the one imported
        # here; matched on the attributes instead of by type.
        candidates = []
        for name, value in vars(JobConfigs).items():
            if name.startswith("_"):
                continue
            candidates.extend(value if isinstance(value, (list, tuple)) else [value])
        jobs = [
            job
            for job in candidates
            if isinstance(getattr(job, "command", None), str)
            and "libfuzzer_test_check.py" in job.command
        ]
        assert jobs, "no job runs the script: the arms below are vacuous"
        return jobs

    def test_the_binary_requirement_follows_the_minimize_only_flag(self):
        # Enumerated over the script rather than named, so a third job that runs
        # it has to make the same choice.
        mismatched = [
            job.name
            for job in self._script_jobs()
            if ("--minimize-only" in job.command)
            == (ArtifactNames.CH_ARM_RELEASE in job.requires)
        ]
        assert mismatched == [], mismatched

    def test_both_arms_of_that_pairing_exist(self):
        # Anti-vacuity: with only one kind of job present the assertion above
        # holds for every implementation.
        flags = {"--minimize-only" in job.command for job in self._script_jobs()}
        assert flags == {True, False}, flags

    def test_the_minimization_workflow_provides_no_release_binary(self):
        # The other half: a job cannot be given an artifact its workflow never
        # produces, so this is what makes the requirement above unsatisfiable.
        provided = {a for job in corpus_workflow.jobs for a in job.provides}
        assert ArtifactNames.CH_ARM_RELEASE not in provided


class TestLongRetentionTags:
    @pytest.mark.parametrize(
        "workflow",
        [
            master_workflow,
            nightly_workflow,
            release_workflow,
            jepsen_workflow,
            sqlancer_workflow,
        ],
        ids=lambda w: w.name,
    )
    def test_long_retention_binaries_are_tagged(self, workflow):
        # Every workflow uploading these has to tag them identically, otherwise
        # one workflow's upload downgrades another's retention.
        by_name = {a.name: a for a in workflow.artifacts}
        untagged = [
            name
            for name in BINARIES_WITH_LONG_RETENTION
            if name in by_name and _tags(by_name[name]) != {"retention": "long"}
        ]
        assert untagged == []

    def test_helper_only_tags_the_listed_binaries(self):
        tagged = {
            a.name
            for a in with_long_retention_tags(ArtifactConfigs.clickhouse_binaries)
            if _tags(a)
        }
        assert tagged == set(BINARIES_WITH_LONG_RETENTION)

    def test_helper_matches_the_loop_it_replaced(self):
        # The two call sites that already had this loop must keep their exact
        # behaviour; a positive control guards against a vacuous comparison of
        # two untagged lists.
        expected = []
        for artifact in ArtifactConfigs.clickhouse_binaries:
            if artifact.name in BINARIES_WITH_LONG_RETENTION:
                artifact = artifact.add_tags({"retention": "long"})
            expected.append(artifact)
        actual = with_long_retention_tags(ArtifactConfigs.clickhouse_binaries)
        assert [dataclasses.asdict(a) for a in actual] == [
            dataclasses.asdict(a) for a in expected
        ]
        assert sum(1 for a in actual if _tags(a)) == len(BINARIES_WITH_LONG_RETENTION)

    def test_every_colliding_workflow_tags_them(self):
        # Enumerated rather than listed by hand so a workflow added later is covered.
        #
        # A pull_request upload is keyed under PRs/<number> and a merge_queue one under
        # the queue's own throwaway ref, so neither shares a key with master's.
        examined, untagged = [], []
        directory = os.path.join(os.path.dirname(__file__), "..", "workflows")
        for name in sorted(os.listdir(directory)):
            if not name.endswith(".py") or name == "__init__.py":
                continue
            module = importlib.import_module(f"ci.workflows.{name[:-3]}")
            for workflow in getattr(module, "WORKFLOWS", []):
                if workflow.event in (
                    Workflow.Event.PULL_REQUEST,
                    Workflow.Event.MERGE_QUEUE,
                ):
                    continue
                declared = {a.name: a for a in workflow.artifacts or []}
                provided = {a for job in workflow.jobs for a in job.provides}
                for binary in BINARIES_WITH_LONG_RETENTION:
                    if binary not in declared or binary not in provided:
                        continue
                    examined.append((workflow.name, binary))
                    if _tags(declared[binary]) != {"retention": "long"}:
                        untagged.append((workflow.name, binary))
        assert examined, "no workflow uploads a long-retention binary: arm is vacuous"
        assert untagged == []

    def test_helper_does_not_mutate_the_shared_configs(self):
        # The artifact configs are module-level singletons shared by every
        # workflow, so tagging has to copy.
        with_long_retention_tags(ArtifactConfigs.clickhouse_binaries)
        assert all(
            _tags(a) is None for a in ArtifactConfigs.clickhouse_binaries
        ), "tagging leaked into the shared ArtifactConfigs"
