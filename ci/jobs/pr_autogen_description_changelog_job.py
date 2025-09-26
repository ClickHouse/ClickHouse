import os

from ci.jobs.scripts.ci_agent import SECTION_CHANGELOG, SECTION_DESCRIPTION, CIAgent
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils


def get_pr_info(diff_file):
    Shell.check("which claude", verbose=True, strict=True)
    Shell.check("gh auth status", strict=True, verbose=True)
    pr_diff = GH.get_pr_diff()
    assert pr_diff
    with open(diff_file, "w", encoding="utf-8") as file:
        file.write(pr_diff)
    return True


def process_description(should_format, diff_file, pr_body_container, format_only):
    if should_format:
        updated_description = agent.format_description_or_changelog(
            "description", current_description
        )
    else:
        updated_description = agent.generate_description(diff_file, info.pr_body)
    assert updated_description
    pr_body_container[0] = agent.insert_content_between_tags(
        pr_body_container[0], SECTION_DESCRIPTION, updated_description, format_only
    )
    return True


def process_changelog(should_format, diff_file, pr_body_container, format_only):
    if should_format:
        updated_changelog = agent.format_description_or_changelog(
            "changelog", current_changelog
        )
    else:
        updated_changelog = agent.generate_changelog_entry(
            diff_file, pr_body_container[0]
        )
    assert updated_changelog
    pr_body_container[0] = agent.insert_content_between_tags(
        pr_body_container[0], SECTION_CHANGELOG, updated_changelog, format_only
    )
    assert updated_changelog in pr_body_container[0]
    return True


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
    diff_file = f"{temp_dir}/diff.txt"

    info = Info()
    agent = CIAgent()

    should_process_description, should_format_description, current_description = (
        agent.should_process_section(SECTION_DESCRIPTION)
    )
    should_process_changelog, should_format_changelog, current_changelog = (
        agent.should_process_section(SECTION_CHANGELOG)
    )

    print(
        f"should_process_description: {should_process_description}, should_format_description: {should_format_description}, current_description: {current_description}"
    )
    print(
        f"should_process_changelog: {should_process_changelog}, should_format_changelog: {should_format_changelog}, current_changelog: {current_changelog}"
    )

    test_results = []
    pr_body_container = [info.pr_body]

    is_ok = True

    if should_process_description or should_process_changelog:
        test_results.append(
            Result.from_commands_run(
                name="get info", command=lambda: get_pr_info(diff_file)
            )
        )
        is_ok = test_results[-1].is_ok()

    if is_ok and should_process_description:
        test_results.append(
            Result.from_commands_run(
                name="process description",
                command=lambda: process_description(
                    should_format_description,
                    diff_file,
                    pr_body_container,
                    should_format_description,
                ),
            )
        )
        is_ok = test_results[-1].is_ok()
    else:
        test_results.append(
            Result(
                name="process description",
                status=Result.Status.SKIPPED,
                info="the section is disabled, not present, or already contains user-provided content.",
            )
        )

    if is_ok and should_process_changelog:
        test_results.append(
            Result.from_commands_run(
                name="process changelog",
                command=lambda: process_changelog(
                    should_format_changelog,
                    diff_file,
                    pr_body_container,
                    should_format_changelog,
                ),
            )
        )
        is_ok = test_results[-1].is_ok()
    else:
        test_results.append(
            Result(
                name="process changelog",
                status=Result.Status.SKIPPED,
                info="the section is disabled, not present, or already contains user-provided content.",
            )
        )

    if is_ok and (should_process_description or should_process_changelog):
        test_results.append(
            Result.from_commands_run(
                name="update PR body",
                command=lambda: GH.update_pr_body(pr_body_container[0]),
            )
        )
    else:
        test_results.append(
            Result(
                name="update PR body",
                status=Result.Status.SKIPPED,
                info="automatic generation or formating disabled",
            )
        )

    Result.create_from(results=test_results).complete_job()
