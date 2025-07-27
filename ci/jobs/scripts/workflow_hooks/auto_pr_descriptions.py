import re
import sys
from praktika.info import Info
from praktika.utils import Shell
from typing import Tuple
from pathlib import Path

def ensure_claude_API_key(info: Info) -> bool:
    try:
        api_key = info.get_secret("ANTHROPIC_API_KEY")
        if not api_key:
            print("Error: ANTHROPIC_API_KEY secret not configured")
            return False
        print("ANTHROPIC_API_KEY secret found")
    except Exception as e:
        print(f"Error: Could not access ANTHROPIC_API_KEY secret: {e}")
        return False

def ensure_claude_code_cli() -> bool:
    try:
        Shell.check("command -v claude")
        print("claude-code CLI already available")
        return True
    except:
        print("claude-code not found, installing...")
        try:
            Shell.check("npm install -g @anthropic-ai/claude-code", verbose=True)
            print("claude-code CLI installed successfully")
            return True
        except:
            print("Error: Could not install claude-code CLI")
            return False

def generate_description(pr_number: int) -> str:
    description = Shell.get_output(
        f'echo "Generate PR description for PR #{pr_number}" | '
        f'claude --model claude-3-haiku-20240307 '
        f'--allowed-tools "Read,Bash" '
        f'--system-prompt "You are a helpful assistant whose job it is to generate '
        f'a pull request description for a given pull request number. '
        f'Try to make the description concise but informative. Return '
        f'only the PR description without any other text. '
        f'You may run \\`gh pr diff {pr_number}\\` to assist you."'
    )
    return description

def generate_changelog(pr_number: int) -> str:
    description = Shell.get_output(
        f'echo "Generate a change log entry for PR #{pr_number}" | '
        f'claude --model claude-3-haiku-20240307 '
        f'--allowed-tools "Read,Bash" '
        f'--system-prompt "You are a helpful assistant whose job it is to generate '
        f'a changelog entry for a given pull request number. '
        f'Try to make the changelog entry concise but informative.'
        f'It should convey the change from the user perspective. '
        f'Return only the changelog entry without any other text. '
        f'You may run \\`gh pr diff {pr_number}\\` to assist you."'
    )
    return description

def check_body_commands(pr_body: str) -> Tuple[bool, bool]:
    lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
    lines = [re.sub(r"\s+", " ", line) for line in lines]

    contains_auto_description = False
    contains_auto_changelog = False
    for line in lines:
        if re.search(r"ci:auto-description", line):
            contains_auto_description = True
        if re.search(r"ci:auto-changelog", line):
            contains_auto_changelog = True
    return (contains_auto_description, contains_auto_changelog)

if __name__ == "__main__":
    info = Info()
    if not ensure_claude_code_cli() or not ensure_claude_API_key(info):
        sys.exit(1)

    description_required_body, changelog_required_body = check_body_commands(info.pr_body)
    if description_required_body:
        description = generate_description(84516)
        print(description)
    if changelog_required_body:
        changelog = generate_changelog(84516)
        print(changelog)