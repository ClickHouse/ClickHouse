from contextlib import redirect_stdout
import io
import re
import unittest

from ci.jobs.scripts.docs import vale_check


GITHUB_WORKFLOW_COMMAND = re.compile(
    r"(?m)^::(?:error|warning|notice|debug|group|endgroup|add-mask|stop-commands)\b"
)
COMPILER_LOCATION = re.compile(r"(?m)^[^|\n]+:\d+:\d+:")


class TestAnnotationSafeOutput(unittest.TestCase):
    def assert_annotation_safe(self, output):
        self.assertIsNone(GITHUB_WORKFLOW_COMMAND.search(output))
        self.assertIsNone(COMPILER_LOCATION.search(output))

    def test_findings_do_not_emit_github_annotations(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            vale_check._report_findings(
                [
                    (
                        "docs/example.mdx",
                        {
                            "Line": 12,
                            "Span": [4, 15],
                            "Match": "this version",
                            "Message": (
                                "::error file=docs/example.mdx,line=12,col=4::bad"
                            ),
                        },
                    )
                ]
            )

        output = stdout.getvalue()
        self.assertIn('document="docs/example.mdx"', output)
        self.assertIn("position=line-12/column-4", output)
        self.assertIn(r"\u2236\u2236error", output)
        self.assert_annotation_safe(output)

    def test_runtime_failures_do_not_emit_github_annotations(self):
        result = type("Result", (), {"returncode": 2})()
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            vale_check._report_runtime_failure(
                result,
                {
                    "runtime_output": (
                        "::warning file=docs/example.mdx,line=3,col=2::bad"
                    )
                },
            )

        output = stdout.getvalue()
        self.assertIn("∶∶warning", output)
        self.assert_annotation_safe(output)


if __name__ == "__main__":
    unittest.main()
