import re

from ci.defs.defs import ArtifactNames
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG


def targeted_variants(jobs, allow_failure=True):
    """Run one repeated targeted check per build and settings configuration."""
    variants = {}
    source_flavors = {}
    for job in jobs:
        source_options = job.parameter.split(", ")
        flavor = next(
            (part for part in source_options if part in ("parallel", "sequential")),
            None,
        )
        options = [
            part
            for part in source_options
            if part not in ("targeted", "parallel", "sequential")
            and not re.fullmatch(r"\d+/\d+", part)
        ]
        parameter = ", ".join([*options, "targeted"])
        variant = job.copy()
        variant.parameter = parameter
        variant.name = f"{job.name.split(' (', 1)[0]} ({parameter})"
        variant.command = job.command.replace(job.parameter, parameter)
        variant.provides = []
        variant.allow_failure = allow_failure
        previous = variants.get(parameter)
        combine_flavors = previous and {source_flavors[parameter], flavor} == {
            "parallel",
            "sequential",
        }
        if previous and (
            previous.runs_on if not combine_flavors else None,
            previous.requires,
            previous.timeout,
            previous.run_in_docker,
        ) != (
            variant.runs_on if not combine_flavors else None,
            variant.requires,
            variant.timeout,
            variant.run_in_docker,
        ):
            raise ValueError(f"Inconsistent shard environments: {parameter}")
        # The parallel flavor's runner is sized for concurrent tests. Keep it
        # when combining both flavors, regardless of the input job order.
        if combine_flavors and flavor == "sequential":
            continue
        variants[parameter] = variant
        source_flavors[parameter] = flavor
    return list(variants.values())


def targeted_matrix(jobs):
    eligible, exemptions = [], {}
    for job in jobs:
        if "llvm_coverage" in job.parameter:
            exemptions[job.name] = (
                "LLVM coverage collection disables randomized settings and owns profdata artifacts"
            )
        elif "azure" in job.parameter:
            exemptions[job.name] = (
                "Azurite runner currently disables randomized settings"
            )
        else:
            eligible.append(job)
    return targeted_variants(eligible), exemptions


def require_selection(jobs):
    return [
        (
            job
            if ArtifactNames.STATELESS_SELECTION in job.requires
            else job.set_requires(ArtifactNames.STATELESS_SELECTION)
        )
        for job in jobs
    ]


def rollout_targeted_jobs(existing, proposed):
    jobs = proposed if SELECTION_CONFIG.expanded_targeted_matrix else existing
    return require_selection(jobs)
