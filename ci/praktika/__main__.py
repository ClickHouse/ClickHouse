import argparse
import sys

from .html_prepare import Html
from .utils import Utils
from .validator import Validator
from .yaml_generator import YamlGenerator


def create_parser():
    parser = argparse.ArgumentParser(prog="praktika")

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    run_parser = subparsers.add_parser("run", help="Job Runner")
    run_parser.add_argument("job", help="Job Name", type=str)
    run_parser.add_argument(
        "--workflow",
        help="Workflow Name (required if job name is not uniq per config)",
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--no-docker",
        help="Do not run job in docker even if job config says so, for local test",
        action="store_true",
    )
    run_parser.add_argument(
        "--docker",
        help="Custom docker image for job run, for local test",
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--param",
        help="Custom parameter to pass into a job script, it's up to job script how to use it, for local test",
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--test",
        help="Custom parameter to pass into a job script, it's up to job script how to use it, for local test",
        type=str,
        default="",
    )
    run_parser.add_argument(
        "--pr",
        help="PR number. Optional parameter for local run. Set if you want an required artifact to be uploaded from CI run in that PR",
        type=int,
        default=None,
    )
    run_parser.add_argument(
        "--sha",
        help="Commit sha. Optional parameter for local run. Set if you want an required artifact to be uploaded from CI run on that sha, head sha will be used if not set",
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--branch",
        help="Commit sha. Optional parameter for local run. Set if you want an required artifact to be uploaded from CI run on that branch, main branch name will be used if not set",
        type=str,
        default=None,
    )
    run_parser.add_argument(
        "--ci",
        help="When not set - dummy env will be generated, for local test",
        action="store_true",
        default="",
    )

    _yaml_parser = subparsers.add_parser("yaml", help="Generates Yaml Workflows")

    _html_parser = subparsers.add_parser("html", help="Uploads HTML page for reports")
    _html_parser.add_argument(
        "--test",
        help="Upload page to test location",
        action="store_true",
        default="",
    )
    return parser


def main():
    sys.path.append(".")
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "yaml":
        Validator().validate()
        YamlGenerator().generate()
    elif args.command == "html":
        Html.prepare(args.test)
    elif args.command == "run":
        from .mangle import _get_workflows
        from .runner import Runner

        workflows = _get_workflows(
            name=args.workflow or None, default=not bool(args.workflow)
        )
        job_workflow_pairs = []
        for workflow in workflows:
            jobs = workflow.find_jobs(args.job, lazy=True)
            if jobs:
                for job in jobs:
                    job_workflow_pairs.append((job, workflow))
        if not job_workflow_pairs:
            Utils.raise_with_error(
                f"Failed to find job [{args.job}] workflow [{args.workflow}]"
            )
        elif len(job_workflow_pairs) > 1:
            for job, wf in job_workflow_pairs:
                print(f"Job: [{job.name}], Workflow [{wf.name}]")
            Utils.raise_with_error(
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
                test=args.test,
                pr=args.pr,
                branch=args.branch,
                sha=args.sha,
            )
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
