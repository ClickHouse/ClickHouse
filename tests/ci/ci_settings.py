import re
from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Any, Iterable

from ci_config import CI
from git_helper import Runner as GitRunner, GIT_PREFIX
from pr_info import PRInfo

# pylint: disable=too-many-return-statements


@dataclass
class CiSettings:
    # job will be included in the run if any keyword from the list matches job name
    include_keywords: Optional[List[str]] = None
    # job will be excluded in the run if any keyword from the list matches job name
    exclude_keywords: Optional[List[str]] = None

    # list of specified preconfigured ci sets to run
    ci_sets: Optional[List[str]] = None
    # list of specified jobs to run
    ci_jobs: Optional[List[str]] = None

    # batches to run for all multi-batch jobs
    job_batches: Optional[List[int]] = None

    do_not_test: bool = False
    no_ci_cache: bool = False
    upload_all: bool = False
    no_merge_commit: bool = False
    woolen_wolfdog: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def create_from_run_config(run_config: Dict[str, Any]) -> "CiSettings":
        return CiSettings(**run_config["ci_settings"])

    @staticmethod
    def create_from_pr_message(
        debug_message: Optional[str], update_from_api: bool
    ) -> "CiSettings":
        """
        Creates CiSettings instance based on tags found in PR body and/or commit message
        @commit_message - may be provided directly for debugging purposes, otherwise it will be retrieved from git.
        """
        res = CiSettings()
        pr_info = PRInfo()
        if (
            not pr_info.is_pr and not debug_message
        ):  # if commit_message is provided it's test/debug scenario - do not return
            # CI options can be configured in PRs only
            # if debug_message is provided - it's a test
            return res
        message = debug_message or GitRunner(set_cwd_to_git_root=True).run(
            f"{GIT_PREFIX} log {pr_info.sha} --format=%B -n 1"
        )

        # CI setting example we need to match with re:
        # - [x] <!---ci_exclude_tsan|msan|ubsan|coverage--> Exclude: All with TSAN, MSAN, UBSAN, Coverage
        pattern = r"(#|- \[x\] +<!---)([|\w]+)"
        matches = [match[-1] for match in re.findall(pattern, message)]
        print(f"CI tags from commit message: [{matches}]")

        if not debug_message:  # to be skipped if debug/test
            pr_info = PRInfo(
                pr_event_from_api=update_from_api
            )  # Fetch updated PR body from GH API
            matches_pr = [match[-1] for match in re.findall(pattern, pr_info.body)]
            print(f"CI tags from PR body: [{matches_pr}]")
            matches = list(set(matches + matches_pr))

            if "do not test" in pr_info.labels:
                # do_not_test could be set in GH labels
                res.do_not_test = True

        for match in matches:
            if match.startswith("job_"):
                if not res.ci_jobs:
                    res.ci_jobs = []
                res.ci_jobs.append(match.removeprefix("job_"))
            elif match.startswith("ci_set_") and match in CI.Tags:
                if not res.ci_sets:
                    res.ci_sets = []
                res.ci_sets.append(match)
            elif match.startswith("ci_include_"):
                if not res.include_keywords:
                    res.include_keywords = []
                res.include_keywords.append(
                    CI.Utils.normalize_string(match.removeprefix("ci_include_"))
                )
            elif match.startswith("ci_exclude_"):
                if not res.exclude_keywords:
                    res.exclude_keywords = []
                keywords = match.removeprefix("ci_exclude_").split("|")
                res.exclude_keywords += [
                    CI.Utils.normalize_string(keyword) for keyword in keywords
                ]
            elif match == CI.Tags.NO_CI_CACHE:
                res.no_ci_cache = True
                print("NOTE: CI Cache will be disabled")
            elif match == CI.Tags.UPLOAD_ALL_ARTIFACTS:
                res.upload_all = True
                print("NOTE: All binary artifacts will be uploaded")
            elif match == CI.Tags.DO_NOT_TEST_LABEL:
                res.do_not_test = True
            elif match == CI.Tags.NO_MERGE_COMMIT:
                res.no_merge_commit = True
                print("NOTE: Merge Commit will be disabled")
            elif match == CI.Tags.WOOLEN_WOLFDOG_LABEL:
                res.woolen_wolfdog = True
                print("NOTE: Woolen Wolfdog mode enabled")
            elif match.startswith("batch_"):
                batches = []
                try:
                    batches = [
                        int(batch) for batch in match.removeprefix("batch_").split("_")
                    ]
                except Exception:
                    print(f"ERROR: failed to parse commit tag [{match}] - skip")
                if batches:
                    if not res.job_batches:
                        res.job_batches = []
                    res.job_batches += batches
                    res.job_batches = list(set(res.job_batches))
            else:
                print(
                    f"WARNING: Invalid tag in commit message or PR body [{match}] - skip"
                )

        return res

    def _check_if_selected(
        self,
        job: str,
        job_config: CI.JobConfig,
        is_release: bool,
        is_pr: bool,
        is_mq: bool,
        labels: Iterable[str],
    ) -> bool:  # type: ignore #too-many-return-statements
        if self.do_not_test:
            label_config = CI.get_tag_config(CI.Tags.DO_NOT_TEST_LABEL)
            assert label_config, f"Unknown tag [{CI.Tags.DO_NOT_TEST_LABEL}]"
            if job in label_config.run_jobs:
                print(
                    f"Job [{job}] present in CI set [{CI.Tags.DO_NOT_TEST_LABEL}] - pass"
                )
                return True
            return False

        if job_config.run_by_label:
            if job_config.run_by_label in labels and is_pr:
                print(
                    f"Job [{job}] selected by GH label [{job_config.run_by_label}] - pass"
                )
                return True
            else:
                return False

        # do not exclude builds
        if self.exclude_keywords and not CI.is_build_job(job):
            for keyword in self.exclude_keywords:
                if keyword in CI.Utils.normalize_string(job):
                    print(f"Job [{job}] matches Exclude keyword [{keyword}] - deny")
                    return False

        to_deny = False
        if self.include_keywords:
            # do not exclude builds
            if job == CI.JobNames.STYLE_CHECK or CI.is_build_job(job):
                # never exclude Style Check by include keywords
                return True
            for keyword in self.include_keywords:
                if keyword in CI.Utils.normalize_string(job):
                    print(f"Job [{job}] matches Include keyword [{keyword}] - pass")
                    return True
            to_deny = True

        if self.ci_sets:
            for tag in self.ci_sets:
                label_config = CI.get_tag_config(tag)
                assert label_config, f"Unknown tag [{tag}]"
                if job in label_config.run_jobs:
                    print(f"Job [{job}] present in CI set [{tag}] - pass")
                    return True
            to_deny = True

        if self.ci_jobs:
            if job in self.ci_jobs:
                print(f"Job [{job}] set by CI #job_ tags [{self.ci_jobs}] - pass")
                return True
            to_deny = True

        if job_config.release_only and not is_release:
            return False
        elif job_config.pr_only and not is_pr and not is_mq:
            return False

        return not to_deny

    def apply(
        self,
        job_configs: Dict[str, CI.JobConfig],
        is_release: bool,
        is_pr: bool,
        is_mq: bool,
        labels: Iterable[str],
    ) -> Dict[str, CI.JobConfig]:
        """
        Apply CI settings from pr body
        """
        res = {}
        for job, job_config in job_configs.items():
            if self._check_if_selected(
                job,
                job_config,
                is_release=is_release,
                is_pr=is_pr,
                is_mq=is_mq,
                labels=labels,
            ):
                res[job] = job_config

        add_parents = []
        for job in list(res):
            parent_jobs = CI.get_job_parents(job)
            for parent_job in parent_jobs:
                if parent_job not in res:
                    add_parents.append(parent_job)
                    print(f"Job [{job}] requires [{parent_job}] - add")
        for job in add_parents:
            res[job] = job_configs[job]

        return res
