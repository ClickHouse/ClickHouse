import os
import sys

from praktika.info import Info

from ci.jobs.scripts.ci_agent import CIAgent
from ci.praktika.gh import GH
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

if __name__ == "__main__":

    # Set up Claude Code environment variables for Bedrock
    os.environ["CLAUDE_CODE_USE_BEDROCK"] = "1"
    os.environ["AWS_REGION"] = "us-east-1"
    os.environ["CLAUDE_CODE_MAX_OUTPUT_TOKENS"] = "4096"
    os.environ["ANTHROPIC_MODEL"] = (
        "arn:aws:bedrock:us-east-1:542516086801:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0"
    )
    os.environ["ANTHROPIC_DEFAULT_HAIKU_MODEL"] = (
        "arn:aws:bedrock:us-east-1:542516086801:inference-profile/us.anthropic.claude-3-5-haiku-20241022-v1:0"
    )
    # https://github.com/anthropics/claude-code/issues/4887
    # os.environ["MAX_THINKING_TOKENS"] = "1024"

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    info = Info()
    agent = CIAgent()

    testname = "Test GitHub authorization"
    results.append(
        Result.from_commands_run(
            name=testname,
            command=["gh auth status"],
        )
    )

    pr_diff = GH.get_pr_diff()

    diff_file = "diff.txt"
    with open(diff_file, "w", encoding="utf-8") as file:
        file.write(pr_diff)

    should_process_description, description_params = agent.should_process_section(
        "BEGIN_DESCRIPTION"
    )
    should_process_changelog, changelog_params = agent.should_process_section(
        "BEGIN_CHANGELOG_ENTRY"
    )

    # Check for cached API errors in the PR body content (from previous failed runs)
    _, description_content = description_params
    _, changelog_content = changelog_params

    if "API Error: " in description_content:
        results.append(
            Result(
                "PR description check",
                Result.Status.FAILED,
                f"Cached API error detected in PR description: {description_content}",
            )
        )
        Result.create_from(results=results).complete_job()
        sys.exit(1)

    if "API Error: " in changelog_content:
        results.append(
            Result(
                "PR changelog check",
                Result.Status.FAILED,
                f"Cached API error detected in PR changelog: {changelog_content}",
            )
        )
        Result.create_from(results=results).complete_job()
        sys.exit(1)

    updated_pr_body = info.pr_body

    if should_process_description:
        should_format, content = description_params
        updated_description = ""
        try:
            if should_format:
                testname = "Format PR description"
                updated_description = agent.format_description_or_changelog(
                    "description", content
                )
                results.append(Result(testname, "OK"))
            else:
                testname = "Autogenerate PR description"
                updated_description = agent.generate_description(
                    diff_file, info.pr_body
                )
                results.append(Result(testname, "OK"))
            updated_pr_body = agent.insert_content_between_tags(
                updated_pr_body, "BEGIN_DESCRIPTION", updated_description
            )
        except RuntimeError as e:
            if "API Error: " in str(e):
                # API error detected - fail the job
                results.append(Result(testname, Result.Status.FAILED, str(e)))
                Result.create_from(results=results).complete_job()
                sys.exit(1)
            raise
    else:
        testname = "Format / autogenerate PR description"
        results.append(
            Result(
                testname,
                Result.Status.SKIPPED,
                "Skipping PR description formatting / autogeneration. Either no tags were found, or the tags have body text but format=false",
            )
        )

    if should_process_changelog:
        should_format, content = changelog_params
        updated_changelog_entry = ""
        try:
            if should_format:
                testname = "Format PR changelog entry"
                updated_changelog_entry = agent.format_description_or_changelog(
                    "changelog", content
                )
                results.append(Result(testname, "OK"))
            else:
                testname = "Autogenerate PR changelog entry"
                updated_changelog_entry = agent.generate_changelog_entry(
                    diff_file, updated_pr_body
                )
                results.append(Result(testname, "OK"))
            updated_pr_body = agent.insert_content_between_tags(
                updated_pr_body, "BEGIN_CHANGELOG_ENTRY", updated_changelog_entry
            )
        except RuntimeError as e:
            if "API Error: " in str(e):
                # API error detected - fail the job
                results.append(Result(testname, Result.Status.FAILED, str(e)))
                Result.create_from(results=results).complete_job()
                sys.exit(1)
            raise
    else:
        testname = "Format / autogenerate changelog entry"
        results.append(
            Result(
                testname,
                Result.Status.SKIPPED,
                "Skipping PR changelog entry formatting / autogeneration. Either no tags were found, or the tags have body text but format=false",
            )
        )

    testname = "Update PR body"
    if should_process_description or should_process_changelog:
        Result.from_commands_run(
            name=testname,
            command=GH.update_pr_body(updated_pr_body),
        )
    else:
        results.append(
            Result(
                testname,
                Result.Status.SKIPPED,
                "No conditions were detected for automatically generating or formatting a changelog entry or a PR description",
            )
        )

    Result.create_from(results=results).complete_job()
