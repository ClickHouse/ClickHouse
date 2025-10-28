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
Generate a formatted PR body for PR #{info.pr_number} following ClickHouse conventions.

## Required Sections

### 1. Changelog Category
Header: "### Changelog category (leave one):"
- If a category is already selected in the PR body: keep only that ONE category
- If no category is selected: include ALL categories below so the user can choose later
- Categories must be copied EXACTLY as written (character-for-character, including parentheses):
- New Feature
- Experimental Feature
- Improvement
- Performance Improvement
- Backward Incompatible Change
- Build/Testing/Packaging Improvement
- Documentation (changelog entry is not required)
- Critical Bug Fix (crash, data loss, RBAC) or LOGICAL_ERROR
- Bug Fix (user-visible misbehavior in an official stable release)
- CI Fix or Improvement (changelog entry is not required)
- Not for changelog (changelog entry is not required)

### 2. Changelog Entry
Header: "### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):"
- Include this section UNLESS the category says "changelog entry is not required"
- Reuse existing entry with grammar fixes, or generate from PR title if missing
- Keep it plain text, ≤50 words, no quotes or markdown formatting
- If PR mentions "Fixes/Resolves/Closes #NUMBER", append: "Resolves #NUMBER"

## Optional Sections

### 3. Documentation Entry
Header: "### Documentation entry for user-facing changes"
- Include only if present in original PR body

### 4. Details
Header: "### Details"
- Add any additional context from the original PR body
- Include issue link if referenced. for example: "Resolves #NUMBER"

## Formatting
- Separate all sections with blank lines
- Remove all markdown comments

## Output
- Write the final PR body to ./ci/tmp/pr_body_generated.md (do not update the actual PR).
- Do not print the PR body to stdout; only brief status logs if needed.
- Do not print non-ANSI characters in the logs to avoid encoding issues.
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
                    f"sed -i.bak '1s/^/<!---AI changelog entry and formatting assistance: false-->\\n/' {output_file} && rm -f {output_file}.bak",
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
