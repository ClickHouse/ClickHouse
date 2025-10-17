import os
import shlex

from ci.praktika import Secret
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

if __name__ == "__main__":
    info = Info()
    assert info.pr_number, "This job must run for a Pull Request"
    PR_BODY_TEMPLATE_PATH = ".github/PULL_REQUEST_TEMPLATE.md"
    output_file = "./ci/tmp/pr_body_generated.md"

    prompt = f"""
You are an autonomous coding agent running inside the ClickHouse repository.

Goal:
- Generate a complete PR body for PR #{info.pr_number} that strictly follows the repository's PR template.

Inputs:
- PR title and body from GitHub.
- Changed files for this PR (and content if needed).
- The PR template at {PR_BODY_TEMPLATE_PATH}. Parse the allowed changelog categories under the section starting with "### Changelog category (leave one):".

Strict template rules:
- Use only these sections, in this order, with exact headings and spacing as in the template:
  1) "### Changelog category (leave one):"
     - Include exactly one bullet with the selected category text (must match an allowed category from the template).
  2) "### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):"
     - Only include if the category is not "changelog entry is not required". If it is, omit this section entirely.
     - Otherwise, provide a single line with the final entry.
     - Follow with a single blank line, then the next header or end of file.
  3) "### Documentation entry for user-facing changes"
     - If this section exists in the original PR body (not the PR template), copy its content as is (preserving checkboxes and text).
     - If not, omit both the header and content.
     - Always remove the section entirely if the category is "Not for change log" or "CI Fix or Improvement"
- Optionally add a final section "### Details" for extra technical details from the existing PR body. Only add this section if there is content to preserve.
- Do not add any other sections or headers.
- Use a single blank line between sections; avoid extra whitespace.

Category selection:
- If the existing PR body contains exactly one allowed category (exact string match), use it.
- If zero or multiple allowed categories are present, derive the category from the changed files and title using these heuristics (then choose the closest exact string from the allowed list):
  * Only docs/ or website/ changes: Documentation (changelog entry is not required).
  * Only ci/, .github/, or tests/ changes: CI Fix or Improvement (changelog entry is not required).
  * Mentions perf/performance: Performance Improvement.
  * Mentions backward + incompat: Backward Incompatible Change.
  * Mentions fix/bug/crash: Bug Fix (user-visible misbehavior in an official stable release), Critical Bug Fix (crash, data loss, RBAC), or LOGICAL_ERROR (pick the closest allowed one).
  * Mentions feature: New Feature or Experimental Feature (pick the closest allowed one).
  * Else: Improvement or Not for changelog (changelog entry is not required), depending on user visibility.
- Always select the category text exactly as it appears in the template; do not invent variants.

Changelog entry rules:
- If the PR body already has a "### Changelog entry" section, extract and improve it if needed:
  * If not sufficiently descriptive, consult changed file paths and minimal file content to rewrite it.
  * Remove markdown, links, and code formatting (plain text only).
  * Make it a single, user-facing sentence in plain English.
  * Limit to a maximum of 50 words.
- If there is no valid entry, generate it from the PR title and change summary with the same constraints (plain text, one sentence, <= 50 words, no quotes or markdown).
- If category is "Not for changelog (changelog entry is not required)", remove the Changelog entry section, including the header.

Details handling:
- Preserve any additional technical details from the existing PR body under a final section titled exactly "### Details".
- Keep edits minimal (fix only obvious grammar, clarity, or formatting issues). Do not move content into other sections. Preserve original formatting: bullet points, type, checkboxes.

Comments:
- Remove all HTML/markdown comments (e.g., <!-- ... -->) from the final output.

Output:
- Write the final PR body to ./ci/tmp/pr_body_generated.md (do not update the actual PR).
- Do not print the PR body to stdout; only brief status logs if needed.

Context:
- Repository: {info.repo_name}
- PR number: {info.pr_number}
- Change URL: {info.change_url}
- Base branch: {info.base_branch}
- Head branch: {info.git_branch}
- Title: {info.pr_title}
- Body: available via the GitHub API.

Execution notes:
- Fetch PR details and changed files via GitHub APIs or gh CLI.
- Read {PR_BODY_TEMPLATE_PATH} from the filesystem to extract the allowed categories. If reading from the filesystem fails, fetch the file from the repository via the GitHub API.
- Be deterministic, avoid placeholders altogether for the changelog entry when it is not required (leave blank), and follow the constraints above exactly.
"""

    res = True
    results = []

    os.environ["GH_TOKEN"] = Secret.Config(
        name="/github-tokens/robot-2-copilot", type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    if res:
        results.append(
            Result.from_commands_run(
                name="prompt",
                command=f"copilot -p {shlex.quote(prompt)} --allow-all-tools",
                with_info=True,
            )
        )
        res = results[-1].is_ok()
    # unset from env to post from default gh app
    os.environ.pop("GH_TOKEN", None)

    if res:
        results.append(
            Result.from_commands_run(
                name="check output",
                command=[
                    f"cat {output_file}",
                    f"test -f {output_file} && test $(wc -l < {output_file}) -gt 1 && test $(wc -c < {output_file}) -gt 50",
                    f"sed -i.bak '1s/^/<!---AI changelog entry and formatting assistance: false-->\\n/' {shlex.quote(output_file)} && rm -f {shlex.quote(output_file)}.bak",
                ],
            )
        )
        res = results[-1].is_ok()
    if res:
        results.append(
            Result.from_commands_run(
                name="update PR body",
                command=[lambda: GH.update_pr_body(body_file=output_file)],
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results).complete_job()
