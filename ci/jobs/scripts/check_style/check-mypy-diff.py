#!/usr/bin/env python3

"""
Script to check mypy errors on files changed from the last common ancestor with master.
Compares mypy errors between the base branch (master) and the current branch.
"""

import subprocess
import sys
import os
import tempfile
import re
from pathlib import Path
from typing import List, Set, Tuple, Dict, Optional
from dataclasses import dataclass


def run_command(cmd: List[str], cwd: Optional[str] = None) -> Tuple[int, str, str]:
    """Run a command and return exit code, stdout, and stderr."""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, capture_output=True, text=True, check=False
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        return 1, "", str(e)


def get_git_root() -> Path:
    """Get the git root directory."""
    exit_code, stdout, _ = run_command(["git", "rev-parse", "--show-toplevel"])
    if exit_code != 0:
        return Path.cwd()
    return Path(stdout.strip())


def get_merge_base_with_master() -> str:
    """Get the merge base commit with master."""
    exit_code, stdout, stderr = run_command(["git", "merge-base", "HEAD", "master"])
    if exit_code != 0:
        print(f"Error finding merge base with master: {stderr}", file=sys.stderr)
        sys.exit(1)
    return stdout.strip()


def get_changed_files(merge_base: str) -> Tuple[List[str], List[str], List[str]]:
    """
    Formally decompose all changes in the diff into three categories:
    1. Modified: files that exist in both branches (same path) with different content
    2. Added: files that exist in current branch but not in base
    3. Deleted: files that exist in base but not in current

    This is a formal decomposition covering ALL changes in the diff.
    Renamed files are treated as: old path -> deleted, new path -> added.

    Returns:
        Tuple of (modified_files, added_files, deleted_files)
    """
    # Get all files in the diff
    exit_code, stdout, stderr = run_command(
        ["git", "diff", "--name-only", merge_base, "HEAD"]
    )
    if exit_code != 0:
        print(f"Error getting diff files: {stderr}", file=sys.stderr)
        sys.exit(1)

    diff_files = set(f.strip() for f in stdout.strip().split("\n") if f.strip())

    # Get all files in base branch
    exit_code, stdout, stderr = run_command(
        ["git", "ls-tree", "-r", "--name-only", merge_base]
    )
    if exit_code != 0:
        print(f"Error getting base branch files: {stderr}", file=sys.stderr)
        sys.exit(1)

    base_files = set(f.strip() for f in stdout.strip().split("\n") if f.strip())

    # Get all files in current branch
    exit_code, stdout, stderr = run_command(
        ["git", "ls-tree", "-r", "--name-only", "HEAD"]
    )
    if exit_code != 0:
        print(f"Error getting current branch files: {stderr}", file=sys.stderr)
        sys.exit(1)

    current_files = set(f.strip() for f in stdout.strip().split("\n") if f.strip())

    # Modified: files in diff that exist in both branches
    modified_files = diff_files & base_files & current_files

    # Added: files in diff that exist in current but not in base
    added_files = diff_files & (current_files - base_files)

    # Deleted: files in diff that exist in base but not in current
    deleted_files = diff_files & (base_files - current_files)

    return sorted(modified_files), sorted(added_files), sorted(deleted_files)


@dataclass
class MyPyResult:
    errors: int


def parse_mypy_error_count(output: str) -> int | None:
    """
    Parse the error count from mypy output.
    Looks for "Success: no issues found" or "Found X error(s) in Y file(s)".

    Args:
        output: The mypy output string

    Returns:
        Number of errors found (0 for success, or the count), or None if pattern not found
    """
    if not output:
        return None

    # First check for success pattern
    # Example: "Success: no issues found in 1 source file"
    success_pattern = r"Success:\s+no\s+issues\s+found"
    if re.search(success_pattern, output, re.IGNORECASE):
        return 0

    # Pattern to match "Found X error(s) in Y file(s)"
    # Examples:
    # "Found 1 error in 1 file (checked 1 source file)"
    # "Found 2 errors in 1 file (checked 1 source file)"
    error_pattern = r"Found\s+(\d+)\s+error"
    match = re.search(error_pattern, output)
    if match:
        return int(match.group(1))

    return None


def run_mypy_on_file(
    file_path: str,
    git_root: Path,
    config_path: Path,
    worktree_path: Optional[Path] = None,
) -> MyPyResult | None:
    """
    Run mypy on a single file.

    Args:
        file_path: Path to the file to check (relative to git_root or worktree_path)
        git_root: Root directory of the git repository
        config_path: Path to mypy config file
        worktree_path: If provided, use this as the base path instead of git_root

    Returns:
        MyPyResult with exit code, output, and parsed error count
    """
    base_path = worktree_path if worktree_path else git_root

    # Determine full path to file
    full_file_path = base_path / file_path
    if not full_file_path.exists():
        return None

    # Build mypy command
    cmd = ["mypy"]
    if config_path.exists():
        cmd.extend(["--config-file", str(config_path)])
    cmd.append("--sqlite-cache")
    cmd.append(str(full_file_path))

    # Run mypy
    exit_code, stdout, stderr = run_command(cmd, cwd=str(base_path))
    output = stdout + stderr

    # Parse error count from output
    error_count = parse_mypy_error_count(output)
    return MyPyResult(errors=error_count)


def run_mypy_on_files_dict(
    file_paths: List[str], git_root: Path, worktree_path: Optional[Path] = None
) -> Dict[str, MyPyResult]:
    """
    Run mypy on multiple files and return a dictionary mapping file path to MyPyResult.

    Args:
        file_paths: List of file paths to check (relative to git_root or worktree_path)
        git_root: Root directory of the git repository
        worktree_path: If provided, use this as the base path instead of git_root

    Returns:
        Dictionary mapping file path (str) to MyPyResult
    """
    base_path = worktree_path if worktree_path else git_root
    config_path = base_path / "tests" / "ci" / ".mypy.ini"

    results = {}
    for file_path in file_paths:
        results[file_path] = run_mypy_on_file(
            file_path, git_root, config_path, worktree_path
        )

    return results


def run_mypy_on_files(
    files: List[str],
    git_root: Path,
    branch: Optional[str] = None,
    worktree_path: Optional[Path] = None,
) -> Tuple[int, str]:
    """
    Run mypy on given files in the specified branch.
    If branch is provided, uses worktree_path as the base directory.
    Returns exit code and output.
    """
    if not files:
        return 0, ""

    base_path = worktree_path if worktree_path else git_root
    config = base_path / "tests" / "ci" / ".mypy.ini"
    if not config.exists():
        print(f"Warning: mypy config not found at {config}", file=sys.stderr)

    # Filter files that exist in the worktree/base
    existing_files = []
    for file in files:
        file_path = base_path / file
        if file_path.exists():
            existing_files.append(str(file_path))

    if not existing_files:
        return 0, ""

    # Build mypy command
    cmd = ["mypy"]
    if config.exists():
        cmd.extend(["--config-file", str(config)])
    cmd.append("--sqlite-cache")
    cmd.extend(existing_files)

    # Run mypy
    exit_code, stdout, stderr = run_command(cmd, cwd=str(base_path))
    return exit_code, stdout + stderr


def normalize_mypy_output(
    output: str, files: List[str], git_root: Optional[Path] = None
) -> Set[str]:
    """
    Normalize mypy output by replacing absolute paths with relative paths.
    Returns a set of normalized error lines.
    """
    lines = output.strip().split("\n")
    normalized = set()

    for line in lines:
        if not line.strip():
            continue
        # Normalize the line by replacing file paths
        normalized_line = line

        # Replace file paths with their relative paths from git root
        for file in files:
            rel_file = file
            # If git_root is provided, try to make paths relative
            if git_root:
                try:
                    # Convert to Path and make relative if it's absolute
                    file_path = Path(file)
                    if file_path.is_absolute():
                        try:
                            rel_file = str(file_path.relative_to(git_root))
                        except ValueError:
                            # If not relative to git_root, keep original
                            rel_file = file
                except Exception:
                    pass

            # Replace absolute paths with relative paths
            # Handle both the original file path and any absolute version
            file_path_obj = Path(file)
            if file_path_obj.is_absolute() and git_root:
                try:
                    abs_in_line = str(file_path_obj)
                    normalized_line = normalized_line.replace(abs_in_line, rel_file)
                except Exception:
                    pass

            # Also replace the relative file path
            normalized_line = normalized_line.replace(file, rel_file)

        normalized.add(normalized_line)

    return normalized


def main():
    git_root = get_git_root()
    os.chdir(git_root)

    # Get merge base with master
    merge_base = get_merge_base_with_master()
    print(f"Merge base with master: {merge_base}")

    # Get all changed files (formal decomposition)
    modified_files, added_files, deleted_files = get_changed_files(merge_base)

    # Filter to only Python files
    modified_files = [f for f in modified_files if f.endswith(".py")]
    added_files = [f for f in added_files if f.endswith(".py")]
    deleted_files = [f for f in deleted_files if f.endswith(".py")]

    if not modified_files and not added_files and not deleted_files:
        print("No Python files changed.")
        return 0

    # Display categorized files
    print("Changed Python files:")
    if modified_files:
        print(f"\n  Modified files ({len(modified_files)}):")
        for f in sorted(modified_files):
            print(f"    - {f}")
    if added_files:
        print(f"\n  Added files ({len(added_files)}):")
        for f in sorted(added_files):
            print(f"    - {f}")
    if deleted_files:
        print(f"\n  Deleted files ({len(deleted_files)}):")
        for f in sorted(deleted_files):
            print(f"    - {f}")
    print()

    # Get current branch name
    exit_code, current_branch, _ = run_command(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"]
    )
    current_branch = current_branch.strip() if exit_code == 0 else "HEAD"

    # Create a worktree for the base branch (master) to compare
    worktree_dir = None
    try:
        # Create temporary worktree for master branch
        worktree_dir = Path(tempfile.mkdtemp(prefix="mypy-check-"))
        print(f"Creating worktree for master branch at {worktree_dir}...")
        exit_code, stdout, stderr = run_command(
            ["git", "worktree", "add", str(worktree_dir), "master"], cwd=git_root
        )
        if exit_code != 0:
            print(f"Error creating worktree: {stderr}", file=sys.stderr)
            sys.exit(1)

        # Run mypy on base branch (master) for modified files
        base_exit_code = 0
        base_output = ""
        if modified_files:
            print("Running mypy on base branch (master) for modified files...")
            base_exit_code, base_output = run_mypy_on_files(
                modified_files, git_root, branch="master", worktree_path=worktree_dir
            )

        # Run mypy on current branch for modified files
        current_exit_code = 0
        current_output = ""
        if modified_files:
            print("Running mypy on current branch for modified files...")
            current_exit_code, current_output = run_mypy_on_files(
                modified_files, git_root, branch=None, worktree_path=None
            )

        # Run mypy on current branch for new files separately
        new_files_exit_code = 0
        new_files_output = ""
        if added_files:
            print("Running mypy on current branch for added files...")
            new_files_exit_code, new_files_output = run_mypy_on_files(
                added_files, git_root, branch=None, worktree_path=None
            )

        # Normalize outputs for comparison (only for modified files that exist in both)
        base_errors = (
            normalize_mypy_output(base_output, modified_files, git_root=worktree_dir)
            if base_output
            else set()
        )
        current_errors = (
            normalize_mypy_output(current_output, modified_files, git_root=git_root)
            if current_output
            else set()
        )

        # Errors in new files
        new_file_errors = (
            normalize_mypy_output(new_files_output, added_files, git_root=git_root)
            if new_files_output
            else set()
        )

        # Compare errors for modified files
        print("\n" + "=" * 80)
        print("MYPY ERROR DIFF")
        print("=" * 80)

        # Errors in modified files
        new_errors_in_modified = current_errors - base_errors
        fixed_errors_in_modified = base_errors - current_errors

        if new_errors_in_modified:
            print(f"\n❌ NEW ERRORS in modified files ({len(new_errors_in_modified)}):")
            print("-" * 80)
            for error in sorted(new_errors_in_modified):
                print(error)

        if fixed_errors_in_modified:
            print(
                f"\n✅ FIXED ERRORS in modified files ({len(fixed_errors_in_modified)}):"
            )
            print("-" * 80)
            for error in sorted(fixed_errors_in_modified):
                print(error)

        # Errors in added files
        if new_file_errors:
            print(f"\n❌ ERRORS in added files ({len(new_file_errors)}):")
            print("-" * 80)
            for error in sorted(new_file_errors):
                print(error)

        # Summary
        total_new_errors = len(new_errors_in_modified) + len(new_file_errors)
        if (
            not new_errors_in_modified
            and not fixed_errors_in_modified
            and not new_file_errors
        ):
            if base_errors:
                print("\n⚠️  No change in errors (same errors as base branch)")
            else:
                print("\n✅ No mypy errors!")

        # Show full output if there are differences
        if (
            total_new_errors > 0
            or (current_exit_code != 0 and base_exit_code == 0)
            or new_files_exit_code != 0
        ):
            print("\n" + "=" * 80)
            print("FULL CURRENT BRANCH OUTPUT:")
            print("=" * 80)
            if current_output:
                print("Modified files:")
                print(current_output)
            if new_files_output:
                print("\nAdded files:")
                print(new_files_output)
            if not current_output and not new_files_output:
                print("(no output)")

        # Exit with error if there are new errors
        exit_code = 0
        if (
            total_new_errors > 0
            or (current_exit_code != 0 and base_exit_code == 0)
            or new_files_exit_code != 0
        ):
            exit_code = 1

        return exit_code

    finally:
        # Cleanup worktree
        if worktree_dir and worktree_dir.exists():
            print(f"\nCleaning up worktree...")
            run_command(
                ["git", "worktree", "remove", "--force", str(worktree_dir)],
                cwd=git_root,
            )
            try:
                if worktree_dir.exists():
                    worktree_dir.rmdir()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
