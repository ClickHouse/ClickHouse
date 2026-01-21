#!/usr/bin/env python3
"""
Script to check current versions of dependencies under contrib/ and compare
with latest available versions from GitHub releases.

Usage:
  python3 check_contrib_versions.py [--no-cache] [--cache-file FILE]

Requirements:
  - gh CLI tool (authenticated)
"""

import re
import subprocess
import sys
import os
import argparse
from dataclasses import dataclass
from typing import Optional
import json


@dataclass
class Submodule:
    """Represents a git submodule"""
    name: str
    path: str
    url: str
    current_commit: Optional[str] = None
    current_commit_date: Optional[str] = None
    current_commit_message: Optional[str] = None
    current_tags: Optional[list[str]] = None
    upstream_url: Optional[str] = None
    latest_release: Optional[dict] = None
    needs_update: Optional[bool] = None
    update_available: Optional[bool] = None


def parse_gitmodules(gitmodules_path: str = ".gitmodules") -> list[Submodule]:
    """Parse .gitmodules file to extract submodule information"""
    submodules = []
    
    with open(gitmodules_path, 'r') as f:
        content = f.read()
    
    # Parse each submodule section
    pattern = r'\[submodule "([^"]+)"\]\s+path = ([^\s]+)\s+url = ([^\s]+)'
    matches = re.findall(pattern, content, re.MULTILINE)
    
    for name, path, url in matches:
        submodules.append(Submodule(name=name, path=path, url=url))
    
    return submodules


def get_submodule_commit(path: str) -> Optional[str]:
    """Get the current commit SHA for a submodule from the parent repo"""
    try:
        result = subprocess.run(
            ['git', 'ls-tree', 'HEAD', path],
            capture_output=True,
            text=True,
            check=True
        )
        # Output format: <mode> <type> <sha> <path>
        parts = result.stdout.strip().split()
        if len(parts) >= 3 and parts[1] == 'commit':
            return parts[2]
    except subprocess.CalledProcessError as e:
        print(f"Error getting commit for {path}: {e}", file=sys.stderr)
    return None


def get_commit_date(path: str, commit_sha: str) -> Optional[str]:
    """Get the date of a commit in a submodule"""
    try:
        result = subprocess.run(
            ['git', '-C', path, 'log', '-1', '--format=%ci', commit_sha],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def get_commit_tags(path: str, commit_sha: str) -> list[str]:
    """Get all tags that point to or contain a commit"""
    try:
        result = subprocess.run(
            ['git', '-C', path, 'tag', '--contains', commit_sha],
            capture_output=True,
            text=True,
            check=True
        )
        return [tag.strip() for tag in result.stdout.strip().split('\n') if tag.strip()]
    except subprocess.CalledProcessError:
        return []


def check_if_commit_is_ancestor(path: str, commit_sha: str, tag: str) -> bool:
    """Check if a commit is an ancestor of a tag (i.e., if tag is newer than commit)"""
    try:
        # git merge-base --is-ancestor <commit> <tag>
        # Returns 0 if commit is ancestor of tag (tag is newer or equal)
        result = subprocess.run(
            ['git', '-C', path, 'merge-base', '--is-ancestor', commit_sha, tag],
            capture_output=True,
            check=False
        )
        return result.returncode == 0
    except Exception:
        return False


def get_commit_message(path: str, commit_sha: str) -> Optional[str]:
    """Get the commit message subject"""
    try:
        result = subprocess.run(
            ['git', '-C', path, 'log', '-1', '--format=%s', commit_sha],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


def extract_repo_info(url: str) -> tuple[Optional[str], Optional[str]]:
    """Extract owner and repo name from GitHub URL"""
    # Handle both https and git URLs
    patterns = [
        r'github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?$',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            owner = match.group(1)
            repo = match.group(2).replace('.git', '')
            return owner, repo
    
    return None, None


def is_clickhouse_fork(url: str) -> bool:
    """Check if a URL is a ClickHouse fork"""
    return 'github.com/ClickHouse/' in url or 'github.com:ClickHouse/' in url


def gh_api_request(endpoint: str) -> Optional[dict]:
    """Make a GitHub API request using gh CLI"""
    try:
        result = subprocess.run(
            ['gh', 'api', endpoint],
            capture_output=True,
            text=True,
            check=True,
            timeout=30
        )
        return json.loads(result.stdout)
    except subprocess.CalledProcessError as e:
        if 'Not Found' in e.stderr or '404' in e.stderr:
            return None
        print(f"   ⚠️  API request failed for {endpoint}: {e.stderr.strip()}", file=sys.stderr)
        return None
    except subprocess.TimeoutExpired:
        print(f"   ⚠️  API request timed out for {endpoint}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse JSON response: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"   ⚠️  Unexpected error: {e}", file=sys.stderr)
        return None


def get_upstream_repo(owner: str, repo: str) -> Optional[tuple[str, str]]:
    """Get the upstream (parent) repo for a fork"""
    data = gh_api_request(f"repos/{owner}/{repo}")
    
    if data and data.get('fork') and data.get('parent'):
        parent = data['parent']
        return parent['owner']['login'], parent['name']
    
    return None


def get_latest_release(owner: str, repo: str) -> Optional[dict]:
    """Get the latest release for a repo"""
    data = gh_api_request(f"repos/{owner}/{repo}/releases/latest")
    
    if data:
        return {
            'tag_name': data.get('tag_name'),
            'name': data.get('name'),
            'published_at': data.get('published_at'),
            'html_url': data.get('html_url'),
            'draft': data.get('draft', False),
            'prerelease': data.get('prerelease', False)
        }
    
    return None


def get_latest_tag(owner: str, repo: str) -> Optional[dict]:
    """Get the latest tag if no releases are available"""
    data = gh_api_request(f"repos/{owner}/{repo}/tags")
    
    if data and isinstance(data, list) and len(data) > 0:
        latest = data[0]
        return {
            'tag_name': latest.get('name'),
            'commit_sha': latest.get('commit', {}).get('sha'),
        }
    
    return None


def load_cache(cache_file: str) -> dict:
    """Load cached data from file"""
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Failed to load cache: {e}", file=sys.stderr)
    return {}


def save_cache(cache_file: str, cache_data: dict):
    """Save cached data to file"""
    try:
        with open(cache_file, 'w') as f:
            json.dump(cache_data, f, indent=2)
    except Exception as e:
        print(f"⚠️  Failed to save cache: {e}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='Check ClickHouse contrib dependencies versions')
    parser.add_argument('--no-cache', action='store_true', help='Do not use cached API responses')
    parser.add_argument('--cache-file', default='.contrib_versions_cache.json', help='Cache file path')
    args = parser.parse_args()
    
    print("=" * 80)
    print("ClickHouse Dependencies Version Check")
    print("=" * 80)
    print()
    
    # Check if gh CLI is available
    try:
        subprocess.run(['gh', 'auth', 'status'], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Error: gh CLI is not installed or not authenticated.")
        print("   Please install gh CLI and authenticate with: gh auth login")
        sys.exit(1)
    
    # Load cache
    cache = {} if args.no_cache else load_cache(args.cache_file)
    
    # Parse .gitmodules
    print("Parsing .gitmodules...")
    submodules = parse_gitmodules()
    print(f"Found {len(submodules)} submodules\n")
    
    # Get current commit for each submodule
    print("Phase 1: Collecting current version information...")
    print()
    
    for i, submodule in enumerate(submodules, 1):
        print(f"[{i}/{len(submodules)}] Processing {submodule.name}...")
        
        submodule.current_commit = get_submodule_commit(submodule.path)
        
        if submodule.current_commit:
            submodule.current_commit_date = get_commit_date(submodule.path, submodule.current_commit)
            submodule.current_commit_message = get_commit_message(submodule.path, submodule.current_commit)
            submodule.current_tags = get_commit_tags(submodule.path, submodule.current_commit)
    
    print()
    print("Phase 2: Fetching upstream and latest release information from GitHub...")
    print()
    
    for i, submodule in enumerate(submodules, 1):
        owner, repo = extract_repo_info(submodule.url)
        
        if not owner or not repo:
            continue
        
        print(f"[{i}/{len(submodules)}] Checking {owner}/{repo}...")
        
        cache_key = f"{owner}/{repo}"
        
        # If it's a ClickHouse fork, try to get upstream
        if is_clickhouse_fork(submodule.url):
            upstream_cache_key = f"upstream:{cache_key}"
            
            if upstream_cache_key in cache:
                upstream = cache[upstream_cache_key]
                print(f"   ↳ Upstream (cached): {upstream}")
            else:
                upstream = get_upstream_repo(owner, repo)
                if upstream:
                    upstream_owner, upstream_repo = upstream
                    cache[upstream_cache_key] = f"{upstream_owner}/{upstream_repo}"
                    if not args.no_cache:
                        save_cache(args.cache_file, cache)
                    upstream = cache[upstream_cache_key]
                    print(f"   ↳ Upstream: {upstream}")
                else:
                    cache[upstream_cache_key] = None
                    if not args.no_cache:
                        save_cache(args.cache_file, cache)
                    print(f"   ⚠️  No upstream found, using fork")
            
            if upstream:
                submodule.upstream_url = f"https://github.com/{upstream}"
                owner, repo = upstream.split('/')
                cache_key = f"{owner}/{repo}"
        
        # Get latest release
        release_cache_key = f"release:{cache_key}"
        
        if release_cache_key in cache:
            release = cache[release_cache_key]
            if release:
                submodule.latest_release = release
                print(f"   ✓ Latest release (cached): {release['tag_name']}")
            else:
                print(f"   ⚠️  No releases or tags found (cached)")
        else:
            release = get_latest_release(owner, repo)
            if release:
                cache[release_cache_key] = release
                if not args.no_cache:
                    save_cache(args.cache_file, cache)
                submodule.latest_release = release
                print(f"   ✓ Latest release: {release['tag_name']}")
            else:
                # Try tags if no release
                tag = get_latest_tag(owner, repo)
                if tag:
                    cache[release_cache_key] = tag
                    if not args.no_cache:
                        save_cache(args.cache_file, cache)
                    submodule.latest_release = tag
                    print(f"   ✓ Latest tag: {tag['tag_name']}")
                else:
                    cache[release_cache_key] = None
                    if not args.no_cache:
                        save_cache(args.cache_file, cache)
                    print(f"   ⚠️  No releases or tags found")
    
    print()
    print("Phase 3: Analyzing version status...")
    print()
    
    # Generate report
    needs_update = []
    up_to_date = []
    no_release_info = []
    commit_only = []
    
    for submodule in submodules:
        owner, repo = extract_repo_info(submodule.url)
        
        if not owner or not repo:
            continue
        
        # If no release info available
        if not submodule.latest_release:
            no_release_info.append(submodule)
            continue
        
        latest_tag = submodule.latest_release.get('tag_name')
        
        # If current commit has tags
        if submodule.current_tags:
            # Check if current commit has the latest release tag
            if latest_tag in submodule.current_tags:
                submodule.update_available = False
                up_to_date.append(submodule)
            else:
                # Current has a tag, but not the latest one
                submodule.update_available = True
                needs_update.append(submodule)
        else:
            # Current commit has no tags - check if it's ahead or behind latest release
            if submodule.current_commit and latest_tag:
                # Try to check if current commit is ancestor of latest tag
                is_behind = check_if_commit_is_ancestor(submodule.path, submodule.current_commit, latest_tag)
                
                if is_behind:
                    # Current commit is an ancestor of latest tag = we're behind
                    submodule.update_available = True
                    needs_update.append(submodule)
                else:
                    # Either ahead or diverged - need manual inspection
                    submodule.update_available = None
                    commit_only.append(submodule)
            else:
                commit_only.append(submodule)
    
    print()
    print("=" * 80)
    print("REPORT: Dependency Status")
    print("=" * 80)
    print()
    
    # Print up to date
    if up_to_date:
        print("✓ UP TO DATE:")
        print("-" * 80)
        for submodule in up_to_date:
            owner, repo = extract_repo_info(submodule.upstream_url if submodule.upstream_url else submodule.url)
            print(f"  {submodule.name}")
            print(f"    Repo: {owner}/{repo}")
            print(f"    Current: {submodule.current_tags[0] if submodule.current_tags else submodule.current_commit[:12]}")
            if submodule.current_commit_date:
                print(f"    Date: {submodule.current_commit_date[:10]}")
            print()
        print()
    
    # Print needs update
    if needs_update:
        print("⚠ UPDATES AVAILABLE:")
        print("-" * 80)
        for submodule in needs_update:
            owner, repo = extract_repo_info(submodule.upstream_url if submodule.upstream_url else submodule.url)
            latest_tag = submodule.latest_release.get('tag_name')
            
            current_display = submodule.current_tags[0] if submodule.current_tags else submodule.current_commit[:12]
            print(f"  {submodule.name}")
            print(f"    Repo: {owner}/{repo}")
            print(f"    Current: {current_display}", end="")
            if submodule.current_commit_date:
                print(f" ({submodule.current_commit_date[:10]})", end="")
            if not submodule.current_tags and submodule.current_commit_message:
                print(f" - {submodule.current_commit_message[:60]}", end="")
            print()
            print(f"    Latest:  {latest_tag}", end="")
            if submodule.latest_release.get('published_at'):
                print(f" ({submodule.latest_release['published_at'][:10]})", end="")
            print()
            if submodule.upstream_url:
                print(f"    URL: {submodule.upstream_url}/releases")
            elif owner and repo:
                print(f"    URL: https://github.com/{owner}/{repo}/releases")
            print()
        print()
    
    # Print commit-only (unclear status)
    if commit_only:
        print("? UNCLEAR STATUS (commit not behind latest tag, may be ahead or diverged):")
        print("-" * 80)
        for submodule in commit_only:
            owner, repo = extract_repo_info(submodule.upstream_url if submodule.upstream_url else submodule.url)
            latest_tag = submodule.latest_release.get('tag_name') if submodule.latest_release else 'Unknown'
            
            print(f"  {submodule.name}")
            print(f"    Repo: {owner}/{repo}")
            print(f"    Current: {submodule.current_commit[:12] if submodule.current_commit else 'Unknown'}", end="")
            if submodule.current_commit_date:
                print(f" ({submodule.current_commit_date[:10]})", end="")
            if submodule.current_commit_message:
                print(f" - {submodule.current_commit_message[:60]}", end="")
            print()
            print(f"    Latest:  {latest_tag}", end="")
            if submodule.latest_release and submodule.latest_release.get('published_at'):
                print(f" ({submodule.latest_release['published_at'][:10]})", end="")
            print()
            print(f"    Note: Commit may be ahead of latest release or on different branch")
            print()
        print()
    
    # Print no release info
    if no_release_info:
        print("? NO RELEASE INFORMATION:")
        print("-" * 80)
        for submodule in no_release_info:
            owner, repo = extract_repo_info(submodule.upstream_url if submodule.upstream_url else submodule.url)
            print(f"  {submodule.name}")
            print(f"    Repo: {owner}/{repo}")
            print(f"    Current: {submodule.current_commit[:12] if submodule.current_commit else 'Unknown'}", end="")
            if submodule.current_commit_date:
                print(f" ({submodule.current_commit_date[:10]})", end="")
            print()
        print()
    
    # Summary
    print("=" * 80)
    print(f"SUMMARY:")
    print(f"  Total submodules: {len(submodules)}")
    print(f"  ✓ Up to date: {len(up_to_date)}")
    print(f"  ⚠ Updates available: {len(needs_update)}")
    print(f"  ? Unclear status: {len(commit_only)}")
    print(f"  ? No release info: {len(no_release_info)}")
    print()
    forked_count = sum(1 for s in submodules if is_clickhouse_fork(s.url))
    print(f"  ClickHouse forks: {forked_count}")
    print(f"  Upstream repos: {len(submodules) - forked_count}")
    print("=" * 80)


if __name__ == "__main__":
    main()
