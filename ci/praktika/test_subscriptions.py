"""
Tests for GitHub Subscriptions Manager
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

# Add parent directory to path to import praktika modules
import sys
sys.path.insert(0, str(Path(__file__).parent))

from subscriptions import GitHubSubscriptions, handle_subscribe_list_command


class TestGitHubSubscriptions(unittest.TestCase):
    """Test cases for GitHubSubscriptions class"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary directory for test config files
        self.test_dir = tempfile.mkdtemp()
        self.config_path = os.path.join(self.test_dir, "subscriptions.json")
        self.manager = GitHubSubscriptions(config_path=self.config_path)
    
    def tearDown(self):
        """Clean up test fixtures"""
        # Remove test directory and files
        if os.path.exists(self.config_path):
            os.remove(self.config_path)
        os.rmdir(self.test_dir)
    
    def test_empty_list(self):
        """Test listing subscriptions when file doesn't exist"""
        repos = self.manager.list_subscriptions()
        self.assertEqual(repos, [])
    
    def test_add_subscription(self):
        """Test adding a repository subscription"""
        result = self.manager.add_subscription("ClickHouse/ClickHouse")
        self.assertTrue(result)
        
        repos = self.manager.list_subscriptions()
        self.assertEqual(repos, ["ClickHouse/ClickHouse"])
    
    def test_add_duplicate_subscription(self):
        """Test adding the same repository twice"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        result = self.manager.add_subscription("ClickHouse/ClickHouse")
        
        self.assertTrue(result)
        repos = self.manager.list_subscriptions()
        self.assertEqual(len(repos), 1)
    
    def test_add_multiple_subscriptions(self):
        """Test adding multiple repositories"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        self.manager.add_subscription("owner/repo1")
        self.manager.add_subscription("owner/repo2")
        
        repos = self.manager.list_subscriptions()
        self.assertEqual(len(repos), 3)
        # Should be sorted
        self.assertEqual(repos[0], "ClickHouse/ClickHouse")
    
    def test_remove_subscription(self):
        """Test removing a repository subscription"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        self.manager.add_subscription("owner/repo1")
        
        result = self.manager.remove_subscription("ClickHouse/ClickHouse")
        self.assertTrue(result)
        
        repos = self.manager.list_subscriptions()
        self.assertEqual(repos, ["owner/repo1"])
    
    def test_remove_nonexistent_subscription(self):
        """Test removing a repository that's not subscribed"""
        result = self.manager.remove_subscription("nonexistent/repo")
        self.assertFalse(result)
    
    def test_format_list_empty(self):
        """Test formatting an empty subscription list"""
        output = self.manager.format_list()
        self.assertEqual(output, "No repositories subscribed.")
    
    def test_format_list_with_repos(self):
        """Test formatting a subscription list with repositories"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        self.manager.add_subscription("owner/repo1")
        
        output = self.manager.format_list()
        self.assertIn("Subscribed to 2 repositories:", output)
        self.assertIn("ClickHouse/ClickHouse", output)
        self.assertIn("owner/repo1", output)
    
    def test_persistence(self):
        """Test that subscriptions persist across manager instances"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        
        # Create a new manager instance with same config path
        new_manager = GitHubSubscriptions(config_path=self.config_path)
        repos = new_manager.list_subscriptions()
        
        self.assertEqual(repos, ["ClickHouse/ClickHouse"])
    
    def test_config_file_format(self):
        """Test that config file has correct JSON format"""
        self.manager.add_subscription("ClickHouse/ClickHouse")
        
        with open(self.config_path, 'r') as f:
            data = json.load(f)
        
        self.assertIn('repositories', data)
        self.assertIn('version', data)
        self.assertEqual(data['version'], '1.0')
        self.assertEqual(data['repositories'], ["ClickHouse/ClickHouse"])
    
    def test_invalid_json_handling(self):
        """Test handling of corrupted JSON file"""
        # Write invalid JSON
        with open(self.config_path, 'w') as f:
            f.write("invalid json {")
        
        repos = self.manager.list_subscriptions()
        self.assertEqual(repos, [])


class TestCommandHandler(unittest.TestCase):
    """Test cases for command handler function"""
    
    def test_handle_subscribe_list_command_integration(self):
        """Test the main command handler"""
        # This test would require mocking the GitHubSubscriptions class
        # or having a real config file in place
        # For now, we just test that it doesn't crash
        result = handle_subscribe_list_command()
        self.assertIsInstance(result, str)


if __name__ == '__main__':
    unittest.main()
