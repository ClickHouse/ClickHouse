import argparse
import sys

from .html_prepare import Html
from .utils import Utils
from .validator import Validator
from .yaml_generator import YamlGenerator


def create_parser():
    parser = argparse.ArgumentParser(
        prog="praktika",
        description=(
            "Praktika CLI: run CI jobs locally or in CI, generate YAML workflows"
        ),
    )

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    run_parser = subparsers.add_parser("run", help="Run a CI job")
    run_parser.add_argument(
        "job",
        help="Name of the job to run",
        type=str,
        nargs="?",
        default=None,
    )
    run_parser.add_argument(
        "--workflow",
        help=(
            "Workflow name to disambiguate when the job name is not unique in the config"
        ),
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--no-docker",
        help=(
            "Run directly on the host even if the job is configured to use Docker (useful for local tests)"
        ),
        action="store_true",
    )
    run_parser.add_argument(
        "--docker",
        help=(
            "Override Docker image to run the job in (e.g. repo/image:tag). Only used when the job runs in Docker"
        ),
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--param",
        help=(
            "Opaque string passed to the job script as --param (job script defines semantics). Useful for local tests"
        ),
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--test",
        help=(
            "One or more values passed to the job script as --test (space-separated) (job script defines semantics). Useful for selecting tests"
        ),
        nargs="+",
        type=str,
        default=[],
    )
    run_parser.add_argument(
        "--path",
        help=(
            "PATH parameter forwarded to the job as --path and mounted into Docker when applicable (job script defines semantics). Useful for local tests"
        ),
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--path_1",
        help=(
            "Additional PATH parameter forwarded to the job as --path and mounted into Docker when applicable (job script defines semantics). Useful for local tests"
        ),
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--count",
        help=(
            "Integer parameter forwarded to the job script (commonly used as number of reruns) (job script defines semantics). Useful for local tests"
        ),
        type=int,
        default=None,
    )
    run_parser.add_argument(
        "--debug",
        help=(
            "Enable debug mode for the job script (passed as --debug) (job script defines semantics). Useful for local tests"
        ),
        action="store_true",
        default="",
    )
    run_parser.add_argument(
        "--workers",
        help=(
            "Integer parameter forwarded to the job script (commonly used as number of parallel workers) (job script defines semantics). Useful for local tests"
        ),
        type=int,
        default=None,
    )
    run_parser.add_argument(
        "--pr",
        help=(
            "PR number to fetch required artifacts from its CI run (for local runs). Optional"
        ),
        type=int,
        default=None,
    )
    run_parser.add_argument(
        "--sha",
        help=(
            "Commit SHA whose CI artifacts should be used for required inputs (for local runs). Defaults to HEAD when not set"
        ),
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--branch",
        help=(
            "Branch name whose CI artifacts should be used for required inputs (for local runs). Defaults to the main branch when not set"
        ),
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--ci",
        help=(
            "Run in CI flag. When not set, a dummy local environment is generated (for local tests)"
        ),
        action="store_true",
        default="",
    )

    _yaml_parser = subparsers.add_parser("yaml", help="Generate YAML workflows")

    _infra_parser = subparsers.add_parser(
        "infrastructure", help="Manage cloud infrastructure and HTML reports"
    )
    _infra_parser.add_argument(
        "--deploy",
        help="Deploy cloud infrastructure or upload HTML report",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--shutdown",
        help="Terminate EC2 instances and/or release Dedicated Hosts",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--all",
        help="Deploy all configured components (used with --deploy)",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--only",
        help=(
            "Process only specified components (e.g. html ImageBuilder LaunchTemplate AutoScalingGroup Lambda DedicatedHost EC2Instance). "
            "With --deploy: deploys only these components or uploads html report. "
            "With --shutdown: releases DedicatedHost or terminates EC2Instance."
        ),
        nargs="+",
        type=str,
        default=None,
    )
    _infra_parser.add_argument(
        "--test",
        help="Test mode for HTML upload (creates _test.html variant)",
        action="store_true",
        default=False,
    )
    return parser


def main():
    sys.path.append(".")
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "yaml":
        Validator().validate()
        YamlGenerator().generate()
    elif args.command == "infrastructure":
        if not args.deploy and not args.shutdown:
            Utils.raise_with_error(
                "infrastructure command requires either --deploy or --shutdown flag"
            )

        if args.deploy:
            # Check if html is in the only list (case-insensitive)
            normalized_only = (
                [c.strip().lower() for c in args.only] if args.only else []
            )
            if normalized_only and "html" in normalized_only:
                Html.prepare(args.test)
                # Remove html from the list for subsequent infrastructure deployment
                remaining_components = [
                    c
                    for c, normalized in zip(args.only, normalized_only)
                    if normalized != "html"
                ]
                if remaining_components:
                    from .mangle import _get_infra_config

                    _get_infra_config().deploy(
                        all=args.all,
                        only=remaining_components,
                    )
            else:
                from .mangle import _get_infra_config

                _get_infra_config().deploy(
                    all=args.all,
                    only=args.only,
                )

        if args.shutdown:
            from .mangle import _get_infra_config

            _get_infra_config().shutdown(
                force=True,
                only=args.only,
            )
    elif args.command == "run":
        from .mangle import _get_workflows
        from .runner import Runner

        workflows = _get_workflows(
            name=args.workflow or None, default=not bool(args.workflow)
        ) # it actually returns only default workflow when there is no --workflow
        if args.job is None:
            for workflow in workflows:
                print(
                    f"Workflow [{workflow.name}] has jobs:\n"
                    "  \"" + f'"\n  "'.join([job.name for job in workflow.jobs]) + '"'
                    )
            Utils.exit_with_error("Job name is required to run a job.")

        job_workflow_pairs = []
        for workflow in workflows:
            jobs = workflow.find_jobs(args.job, lazy=True)
            if jobs:
                for job in jobs:
                    job_workflow_pairs.append((job, workflow))
        if not job_workflow_pairs:
            Utils.exit_with_error(
                f"Failed to find job [{args.job}] workflow [{args.workflow}]"
            )
        elif len(job_workflow_pairs) > 1:
            for job, wf in job_workflow_pairs:
                print(f"Job: [{job.name}], Workflow [{wf.name}]")
            Utils.exit_with_error(
                f"More than one job [{args.job}]: {[(wf.name, job.name) for job, wf in job_workflow_pairs]}"
            )
        else:
            job, workflow = job_workflow_pairs[0][0], job_workflow_pairs[0][1]
            print(f"Going to run job [{job.name}], workflow [{workflow.name}]")
            Runner().run(
                workflow=workflow,
                job=job,
                docker=args.docker,
                local_run=not args.ci,
                no_docker=args.no_docker,
                param=args.param,
                test=" ".join(args.test),
                pr=args.pr,
                branch=args.branch,
                sha=args.sha,
                count=args.count,
                debug=args.debug,
                path=args.path,
                path_1=args.path_1,
                workers=args.workers,
            )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
