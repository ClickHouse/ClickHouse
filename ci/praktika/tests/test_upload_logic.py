
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from praktika.result import Result, _ResultS3
from praktika.s3 import S3
from praktika.settings import Settings
from praktika._environment import _Environment

class TestUploadLogic(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path("/tmp/test_upload_logic")
        self.test_dir.mkdir(parents=True, exist_ok=True)
        
        # Create dummy file structure
        # /tmp/test_upload_logic/
        #   report/
        #     index.html
        #     css/
        #       style.css
        #   other.log
        
        (self.test_dir / "report").mkdir(exist_ok=True)
        (self.test_dir / "report/css").mkdir(exist_ok=True)
        
        self.index_html = self.test_dir / "report/index.html"
        self.index_html.write_text("<html></html>")
        
        self.style_css = self.test_dir / "report/css/style.css"
        self.style_css.write_text("body {}")
        
        self.other_log = self.test_dir / "other.log"
        self.other_log.write_text("log content")

    def tearDown(self):
        import shutil
        if self.test_dir.exists():
            shutil.rmtree(self.test_dir)

    @patch("ci.praktika.s3.S3.parallel_upload")
    @patch("ci.praktika.result._Environment")
    def test_upload_paths(self, mock_env_cls, mock_parallel_upload):
        # Setup mocks
        mock_env = MagicMock()
        mock_env_cls.get.return_value = mock_env
        mock_env.get_s3_prefix.return_value = "s3_prefix"
        
        # Create Result
        # Scenario: index.html is a FILE (downloadable report entry point)
        # style.css is an ASSET (relative logic)
        # result name "TestResult"
        result = Result.create_from(name="TestResult")
        result.files = [str(self.index_html), str(self.other_log)]
        result.assets = [str(self.style_css)]
        
        # Perform upload
        # We expect a call to parallel_upload with specs
        
        _ResultS3.upload_result_files_to_s3(result)
        
        self.assertTrue(mock_parallel_upload.called)
        upload_specs = mock_parallel_upload.call_args[0][0]
        
        # Analyze specs
        specs_by_local = {spec['local_path']: spec for spec in upload_specs}
        
        # 1. Check index.html (FILE)
        # Should be flattened to .../s3_prefix/test_result/index.html
        idx_spec = specs_by_local.get(str(self.index_html))
        self.assertIsNotNone(idx_spec)
        self.assertTrue(idx_spec['s3_path'].endswith("/s3_prefix/test_result/index.html"))
        self.assertFalse(idx_spec['compress']) # Files default no compress unless huge
        
        # 2. Check other.log (FILE)
        # Flattened
        log_spec = specs_by_local.get(str(self.other_log))
        self.assertIsNotNone(log_spec)
        self.assertTrue(log_spec['s3_path'].endswith("/s3_prefix/test_result/other.log"))
        
        # 3. Check style.css (ASSET)
        # Should be relative to "report" (common root of index.html and css/style.css)
        # The common root of [report/index.html, report/css/style.css, other.log] is /tmp/test_upload_logic.
        # Wait, if `other.log` is included, the common root moves up!
        # /tmp/test_upload_logic/report vs /tmp/test_upload_logic/other.log -> common: /tmp/test_upload_logic.
        
        # So relative path for style.css: "report/css/style.css".
        # S3 path: .../s3_prefix/test_result/report/css/style.css
        
        css_spec = specs_by_local.get(str(self.style_css))
        self.assertIsNotNone(css_spec)
        self.assertTrue(css_spec['s3_path'].endswith("/s3_prefix/test_result/report/css/style.css"))
        self.assertTrue(css_spec['compress']) # Assets compress=True
        
        print("\nVerified Specs:")
        for spec in upload_specs:
            print(f"{Path(spec['local_path']).name} -> {spec['s3_path']} (Compress: {spec.get('compress')})")

    @patch("ci.praktika.s3.S3.parallel_upload")
    @patch("ci.praktika.result._Environment")
    def test_upload_paths_clean(self, mock_env_cls, mock_parallel_upload):
        # Scenario without "other.log" to see if common root is tighter
        mock_env = MagicMock()
        mock_env_cls.get.return_value = mock_env
        mock_env.get_s3_prefix.return_value = "s3_prefix"
        
        result = Result.create_from(name="CleanResult")
        result.files = [str(self.index_html)]
        result.assets = [str(self.style_css)]
        
        _ResultS3.upload_result_files_to_s3(result)
        
        upload_specs = mock_parallel_upload.call_args[0][0]
        specs_by_local = {spec['local_path']: spec for spec in upload_specs}
        
        # Common root should be .../report
        # relpath(style.css, report) -> css/style.css
        
        css_spec = specs_by_local.get(str(self.style_css))
        self.assertTrue(css_spec['s3_path'].endswith("/s3_prefix/clean_result/css/style.css"))

if __name__ == "__main__":
    unittest.main()
