import re

from ci.defs.defs import ArtifactNames
from ci.jobs.scripts.test_selection_config import SELECTION_CONFIG


def selection_variants(jobs, option):
    """Collapse shards and preserve each concrete configuration's environment."""
    variants = {}
    for job in jobs:
        options = [
            part
            for part in job.parameter.split(", ")
            if part != "selected tests" and not re.fullmatch(r"\d+/\d+", part)
        ]
        parameter = ", ".join([*options, option])
        variant = job.copy()
        variant.parameter = parameter
        variant.name = f"{job.name.split(' (', 1)[0]} ({parameter})"
        variant.command = job.command.replace(job.parameter, parameter)
        variant.provides = []
        if option == "targeted":
            variant.allow_failure = True
        previous = variants.get(parameter)
        if previous and (
            previous.runs_on,
            previous.requires,
            previous.timeout,
            previous.run_in_docker,
        ) != (
            variant.runs_on,
            variant.requires,
            variant.timeout,
            variant.run_in_docker,
        ):
            raise ValueError(f"Inconsistent shard environments: {parameter}")
        variants[parameter] = variant
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
    return selection_variants(eligible, "targeted"), exemptions


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
