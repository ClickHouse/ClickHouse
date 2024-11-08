import argparse
import sys

from praktika.html_prepare import Html
from praktika.utils import Utils
from praktika.validator import Validator
from praktika.yaml_generator import YamlGenerator


def create_parser():
    parser = argparse.ArgumentParser(prog="python3 -m praktika")

    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    run_parser = subparsers.add_parser("run", help="Job Runner")
    run_parser.add_argument("--job", help="Job Name", type=str, required=True)
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
        "--ci",
        help="When not set - dummy env will be generated, for local test",
        action="store_true",
        default="",
    )

    _yaml_parser = subparsers.add_parser("yaml", help="Generates Yaml Workflows")

    _html_parser = subparsers.add_parser("html", help="Uploads HTML page for reports")

    return parser


if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()

    if args.command == "yaml":
        Validator().validate()
        YamlGenerator().generate()
    elif args.command == "html":
        Html.prepare()
    elif args.command == "run":
        from praktika.mangle import _get_workflows
        from praktika.runner import Runner

        workflows = _get_workflows(name=args.workflow or None)
        job_workflow_pairs = []
        for workflow in workflows:
            job = workflow.find_job(args.job, lazy=True)
            if job:
                job_workflow_pairs.append((job, workflow))
        if not job_workflow_pairs:
            Utils.raise_with_error(
                f"Failed to find job [{args.job}] workflow [{args.workflow}]"
            )
        elif len(job_workflow_pairs) > 1:
            Utils.raise_with_error(
                f"More than one job [{args.job}] found - try specifying workflow name with --workflow"
            )
        else:
            job, workflow = job_workflow_pairs[0][0], job_workflow_pairs[0][1]
            print(f"Going to run job [{job.name}], workflow [{workflow.name}]")
            Runner().run(
                workflow=workflow,
                job=job,
                docker=args.docker,
                dummy_env=not args.ci,
                no_docker=args.no_docker,
                param=args.param,
            )
    else:
        parser.print_help()
        sys.exit(1)
