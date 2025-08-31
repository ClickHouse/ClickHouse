from ci.praktika.result import Result
from ci.praktika.utils import Utils, Shell
from ci.jobs.scripts.ci_agent import CIAgent
from praktika.info import Info
from ci.praktika.gh import GH

if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"
    
    info = Info()
    agent = CIAgent()
    
    testname = "Test GitHub authorization"
    exit_code, out, err = Shell.get_res_stdout_stderr("gh auth status", verbose=False)
    res = out or err
    results.append(Result(testname, res))
    
    pr_diff = GH.get_pr_diff()

    diff_file = "diff.txt"
    with open(diff_file, "w", encoding="utf-8") as file:
        file.write(pr_diff)

    should_process_description, description_params = agent.should_process_section("BEGIN_DESCRIPTION")
    should_process_changelog, changelog_params = agent.should_process_section("BEGIN_CHANGELOG_ENTRY")

    updated_pr_body = info.pr_body

    if should_process_description:
        should_format, content = description_params
        updated_description = ""
        if should_format:
            testname = "Format PR description"
            updated_description = agent.format_description_or_changelog("description", content)
            results.append(Result(testname, "OK"))
        else:
            testname = "Autogenerate PR description"
            updated_description = agent.generate_description(diff_file, info.pr_body)
            results.append(Result(testname, "OK"))
        updated_pr_body = agent.insert_content_between_tags(updated_pr_body, "BEGIN_DESCRIPTION", updated_description)
    else:
        testname = "Format / autogenerate PR description"
        results.append(Result(testname, "SKIPPED", "Skipping PR description formatting / autogeneration. Either no tags were found, or the tags have body text but format=false"))
    
    if should_process_changelog:
        should_format, content = changelog_params
        updated_changelog_entry = ""
        if should_format:
            testname = "Format PR changelog entry"
            updated_changelog_entry = agent.format_description_or_changelog("changelog", content)
            results.append(Result(testname, "OK"))
        else:
            testname = "Autogenerate PR changelog entry"
            updated_changelog_entry = agent.generate_changelog_entry(diff_file, updated_pr_body)
            results.append(Result(testname, "OK"))
        updated_pr_body = agent.insert_content_between_tags(updated_pr_body, "BEGIN_CHANGELOG_ENTRY", updated_changelog_entry)
    else:
        testname = "Format / autogenerate changelog entry"
        results.append(Result(testname, "SKIPPED", "Skipping PR changelog entry formatting / autogeneration. Either no tags were found, or the tags have body text but format=false"))

    testname = "Update PR body"
    if should_process_description or should_process_changelog:
        Result.from_commands_run(
                name=testname,
                command=GH.update_pr_body(updated_pr_body),
            )
    else:
        results.append(Result(testname, "SKIPPED", "No conditions were detected for automatically generating or formatting a changelog entry or a PR description"))

    Result.create_from(results=results).complete_job()
