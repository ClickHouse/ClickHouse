"""
GitHub Repository Subscriptions Manager

This module provides functionality to manage GitHub repository subscriptions.
It allows listing subscribed repositories, which can be useful for CI/CD workflows
and automation scripts that need to track which repositories are being monitored.
"""

import json
import os
import subprocess
from typing import List, Optional
from pathlib import Path


class GitHubSubscriptions:
    """Manager for GitHub repository subscriptions."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the subscriptions manager.
        
        Args:
            config_path: Optional path to subscriptions config file.
                        Defaults to .github/subscriptions.json
        """
        if config_path:
            self.config_path = Path(config_path)
        else:
            # Default to .github/subscriptions.json in the repository root
            try:
                result = subprocess.run(
                    ["git", "rev-parse", "--show-toplevel"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                repo_root = result.stdout.strip()
            except subprocess.CalledProcessError:
                # Fall back to current directory if not in a git repo
                repo_root = os.getcwd()
            self.config_path = Path(repo_root) / ".github" / "subscriptions.json"
    
    def list_subscriptions(self) -> List[str]:
        """
        List all subscribed repositories.
        
        Returns:
            List of repository names in owner/repo format.
        """
        if not self.config_path.exists():
            return []
        
        try:
            with open(self.config_path, 'r') as f:
                data = json.load(f)
                return data.get('repositories', [])
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Failed to read subscriptions config: {e}")
            return []
    
    def add_subscription(self, repo: str) -> bool:
        """
        Add a repository to subscriptions.
        
        Args:
            repo: Repository name in owner/repo format
            
        Returns:
            True if added successfully, False otherwise
        """
        subscriptions = self.list_subscriptions()
        
        if repo in subscriptions:
            print(f"Repository {repo} is already subscribed")
            return True
        
        subscriptions.append(repo)
        return self._save_subscriptions(subscriptions)
    
    def remove_subscription(self, repo: str) -> bool:
        """
        Remove a repository from subscriptions.
        
        Args:
            repo: Repository name in owner/repo format
            
        Returns:
            True if removed successfully, False otherwise
        """
        subscriptions = self.list_subscriptions()
        
        if repo not in subscriptions:
            print(f"Repository {repo} is not in subscriptions")
            return False
        
        subscriptions.remove(repo)
        return self._save_subscriptions(subscriptions)
    
    def _save_subscriptions(self, repositories: List[str]) -> bool:
        """
        Save subscriptions to config file.
        
        Args:
            repositories: List of repository names
            
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            # Ensure directory exists
            self.config_path.parent.mkdir(parents=True, exist_ok=True)
            
            data = {
                'repositories': sorted(repositories),
                'version': '1.0'
            }
            
            with open(self.config_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            return True
        except IOError as e:
            print(f"Error: Failed to save subscriptions: {e}")
            return False
    
    def format_list(self) -> str:
        """
        Format subscription list for display.
        
        Returns:
            Formatted string representation of subscriptions
        """
        repos = self.list_subscriptions()
        
        if not repos:
            return "No repositories subscribed."
        
        result = f"Subscribed to {len(repos)} repositories:\n"
        for repo in repos:
            result += f"  - {repo}\n"
        
        return result.rstrip()


def handle_subscribe_list_command() -> str:
    """
    Handler for '/github subscribe list' command.
    
    Returns:
        Formatted list of subscribed repositories
    """
    manager = GitHubSubscriptions()
    return manager.format_list()


if __name__ == "__main__":
    # CLI interface for testing
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python subscriptions.py <command> [args]")
        print("Commands:")
        print("  list              - List all subscriptions")
        print("  add <repo>        - Add a repository subscription")
        print("  remove <repo>     - Remove a repository subscription")
        sys.exit(1)
    
    command = sys.argv[1]
    manager = GitHubSubscriptions()
    
    if command == "list":
        print(manager.format_list())
    elif command == "add" and len(sys.argv) == 3:
        repo = sys.argv[2]
        if manager.add_subscription(repo):
            print(f"Successfully subscribed to {repo}")
        else:
            print(f"Failed to subscribe to {repo}")
            sys.exit(1)
    elif command == "remove" and len(sys.argv) == 3:
        repo = sys.argv[2]
        if manager.remove_subscription(repo):
            print(f"Successfully unsubscribed from {repo}")
        else:
            print(f"Failed to unsubscribe from {repo}")
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
