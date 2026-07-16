import argparse
import shlex
import sys

from .project_init import run_init_interactive
from .utils import Utils
from .validator import Validator
from .yaml_generator import YamlGenerator


def create_parser():
    parser = argparse.ArgumentParser(
        prog="praktika",
        description=(
            "Praktika is a self-hosted CI system for defining pipelines and infrastructure in Python."
        ),
    )

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    run_parser = subparsers.add_parser("run", help="Run a job")
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
    run_parser.add_argument(
        "--timestamp",
        help="Prefix each output line with a [YYYY-MM-DD HH:MM:SS] timestamp",
        action="store_true",
        default=False,
    )

    subparsers.add_parser(
        "init",
        help="Initialize Praktika CI for a new project",
    )

    _infra_parser = subparsers.add_parser(
        "infrastructure", help="Manage CI infrastructure components"
    )
    _infra_parser.add_argument(
        "--deploy",
        help="Deploy cloud infrastructure or upload HTML report",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--destroy-runtime",
        help="Delete project-prefixed recreatable runtime resources while keeping S3, CIDB/EC2, Dedicated Hosts, secrets/params, and GitHub API Gateway wiring",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--destroy-all",
        help="Delete all project-prefixed managed infrastructure resources. Requires --project.",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--all",
        help="With --deploy: deploy all configured components",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--only",
        help=(
            "Process only specified components (e.g. html ImageBuilder AMI VPC LaunchTemplate AutoScalingGroup Lambda DedicatedHost EC2Instance). "
            "With --deploy: deploys only these components or uploads html report. "
            "With --destroy-runtime/--destroy-all: deletes only the selected component types."
        ),
        nargs="+",
        type=str,
        default=None,
    )
    _infra_parser.add_argument(
        "--restart-instances",
        help="Trigger an instance refresh on all ASGs, replacing all EC2 instances with the current launch template version",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "--project",
        help="Infrastructure project name from ci/infrastructure/projects.py PROJECTS",
        type=str,
        default="",
    )
    _infra_parser.add_argument(
        "--test",
        help="Test mode for HTML upload (creates _test.html variant)",
        action="store_true",
        default=False,
    )
    _infra_parser.add_argument(
        "-y",
        "--yes",
        help="Automatically answer yes to interactive confirmations",
        action="store_true",
        default=False,
    )

    orch_parser = subparsers.add_parser(
        "orchestrate", help="Run local workflow orchestration"
    )
    orch_parser.set_defaults(_command_parser=orch_parser)
    orch_sub = orch_parser.add_subparsers(dest="orch_command")

    wf_parser = orch_sub.add_parser(
        "workflow", help="Orchestrate all matching workflows for a trigger event"
    )
    wf_parser.add_argument(
        "event_file", nargs="?", default=None,
        help="Path to trigger event JSON (auto-generated from git if omitted)",
    )
    wf_parser.add_argument("--event-type", default="pull_request",
        choices=["pull_request", "push"])
    wf_parser.add_argument("--repo", default=None)
    wf_parser.add_argument("--head-sha", default=None)
    wf_parser.add_argument("--head-ref", default=None)
    wf_parser.add_argument("--base-ref", default="main")
    wf_parser.add_argument("--pr-number", default=None, type=int)
    wf_parser.add_argument("--sender", default=None)
    wf_parser.add_argument(
        "--name",
        default=None,
        help="Workflow name to run when more than one workflow matches the event",
    )
    wf_parser.add_argument("--ci", action="store_true", default=False,
        help="CI mode: authenticate to GitHub and post check runs")
    wf_parser.add_argument(
        "--settings",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "Override a Settings value for this run (repeatable). KEY is the "
            "Settings attribute name as in the config, e.g. --settings "
            "AWS_REGION=eu-north-1. Workflow-level AI configuration lives on "
            "Workflow.Config.ai_orchestrator rather than Settings."
        ),
    )

    job_parser = orch_sub.add_parser(
        "job", help="Run a single job from a task JSON"
    )
    job_parser.add_argument("task_file", help="Path to task JSON file")
    job_parser.add_argument("--ci", action="store_true", default=False,
        help="CI mode: authenticate to GitHub and post check run updates")

    subparsers.add_parser(
        "yaml",
        help="Generate YAML for GitHub Actions-based Praktika pipelines",
    )
    return parser


def main(argv=None):
    sys.path.append(".")
    parser = create_parser()
    args = parser.parse_args(argv)

    if args.command == "init":
        run_init_interactive()
    elif args.command == "yaml":
        Validator().validate()
        YamlGenerator().generate()
    elif args.command == "infrastructure":
        from .interactive import UserPrompt

        project = getattr(args, "project", None) or None
        previous_auto_confirm = UserPrompt.AUTO_CONFIRM
        UserPrompt.AUTO_CONFIRM = bool(getattr(args, "yes", False))
        try:
            if (
                not args.deploy
                and not args.destroy_runtime
                and not args.destroy_all
                and not args.restart_instances
            ):
                Utils.raise_with_error(
                    "infrastructure command requires --deploy, --destroy-runtime, --destroy-all, or --restart-instances"
                )
            if args.destroy_runtime and args.destroy_all:
                Utils.raise_with_error(
                    "Use either --destroy-runtime or --destroy-all, not both"
                )

            if args.deploy:
                from .mangle import _get_infra_config

                infra_config = _get_infra_config(project)
                Validator().validate_infrastructure_deploy(infra_config)
                infra_config.deploy(
                    all=args.all,
                    only=args.only,
                    is_test=args.test,
                )

            if args.destroy_runtime:
                from .mangle import _get_infra_config

                if args.all:
                    Utils.raise_with_error(
                        "Use --destroy-all instead of --destroy-runtime --all"
                    )
                _get_infra_config(project, require_project=True).destroy_runtime(
                    force=True,
                    only=args.only,
                )

            if args.destroy_all:
                from .mangle import _get_infra_config

                _get_infra_config(project, require_project=True).destroy_all(
                    only=args.only,
                )

            if args.restart_instances:
                from .mangle import _get_infra_config

                _get_infra_config(project).restart_instances()
        finally:
            UserPrompt.AUTO_CONFIRM = previous_auto_confirm
    elif args.command == "orchestrate":
        if args.orch_command == "workflow":
            from .orchestrator import run as orchestrate_run
            orchestrate_run(args.event_file, args)
        elif args.orch_command == "job":
            import json as _json
            from .orchestrator.job_runner import run_job
            with open(args.task_file) as f:
                task = _json.load(f)
            sys.exit(run_job(task, local=not args.ci))
        else:
            args._command_parser.print_help()
            sys.exit(1)
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
                    '  "' + '"\n  "'.join([job.name for job in workflow.jobs]) + '"'
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
                local_job_run=not args.ci,
                no_docker=args.no_docker,
                param=args.param,
                # Quote each --test value individually: an integration test
                # node ID can contain spaces, parentheses and quotes when the
                # test is parametrized with SQL (e.g.
                # `test.py::t[SELECT now() FROM numbers(2)]`). The runner
                # interpolates this string into a shell command, so each value
                # must survive as a single, unmangled argument.
                test=" ".join(shlex.quote(t) for t in args.test),
                pr=args.pr,
                branch=args.branch,
                sha=args.sha,
                count=args.count,
                debug=args.debug,
                path=args.path,
                path_1=args.path_1,
                workers=args.workers,
                timestamp=args.timestamp,
            )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
