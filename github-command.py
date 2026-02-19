#!/usr/bin/env python3
"""
GitHub Command Handler

This script provides a command-line interface for GitHub-related commands,
including the '/github subscribe list' command functionality.
"""

import sys
from pathlib import Path

# Add parent directory to path to import praktika modules
sys.path.insert(0, str(Path(__file__).parent / "ci"))

from praktika.gh import GH


def print_usage():
    """Print usage information"""
    print("GitHub Command Handler")
    print()
    print("Usage: github-command.py <command> [args]")
    print()
    print("Commands:")
    print("  subscribe list              - List all subscribed repositories")
    print("  subscribe add <repo>        - Add a repository subscription")
    print("  subscribe remove <repo>     - Remove a repository subscription")
    print()
    print("Examples:")
    print("  python github-command.py subscribe list")
    print("  python github-command.py subscribe add ClickHouse/ClickHouse")
    print("  python github-command.py subscribe remove owner/repo")


def handle_subscribe_command(args):
    """Handle subscribe subcommands"""
    if not args:
        print("Error: Missing subscribe subcommand")
        print()
        print_usage()
        return 1
    
    subcommand = args[0]
    
    if subcommand == "list":
        # Handle '/github subscribe list' command
        print(GH.list_subscriptions())
        return 0
    
    elif subcommand == "add":
        if len(args) < 2:
            print("Error: Missing repository name")
            print("Usage: subscribe add <owner/repo>")
            return 1
        
        repo = args[1]
        if GH.add_subscription(repo):
            print(f"Successfully subscribed to {repo}")
            return 0
        else:
            print(f"Failed to subscribe to {repo}")
            return 1
    
    elif subcommand == "remove":
        if len(args) < 2:
            print("Error: Missing repository name")
            print("Usage: subscribe remove <owner/repo>")
            return 1
        
        repo = args[1]
        if GH.remove_subscription(repo):
            print(f"Successfully unsubscribed from {repo}")
            return 0
        else:
            print(f"Failed to unsubscribe from {repo}")
            return 1
    
    else:
        print(f"Error: Unknown subscribe subcommand: {subcommand}")
        print()
        print_usage()
        return 1


def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print_usage()
        return 1
    
    command = sys.argv[1]
    args = sys.argv[2:]
    
    if command == "subscribe":
        return handle_subscribe_command(args)
    else:
        print(f"Error: Unknown command: {command}")
        print()
        print_usage()
        return 1


if __name__ == "__main__":
    sys.exit(main())
