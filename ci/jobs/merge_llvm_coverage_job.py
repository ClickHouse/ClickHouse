import os
import subprocess
from pathlib import Path
from ci.praktika.result import Result
from ci.praktika.utils import Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp/"

if __name__ == "__main__":
    # Pass workspace path to the shell script via environment variable
    os.environ["WORKSPACE_PATH"] = current_directory

    result = Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/scripts/merge_llvm_coverage.sh"],
    )
    # Compress coverage artifacts
    # Change to temp_dir so archive doesn't include full path
    subprocess.run(
        f"cd {temp_dir} && tar -czf llvm_coverage_html_report.tar.gz merged.profdata llvm_coverage_html_report",
        shell=True,
        check=True
    )
    
    files_to_attach = []
    # Attach all HTML report files preserving directory structure
    html_report_dir = Path(temp_dir) / "llvm_coverage_html_report"
    if html_report_dir.exists():
        # Add index.html first as it's the entry point
        index_file = html_report_dir / "index.html"
        if index_file.exists():
            files_to_attach.append("llvm_coverage_html_report/index.html")
        
        # Add all other files
        for file_path in html_report_dir.rglob("*"):
            if file_path.is_file() and file_path.name != "index.html":
                # Get relative path from html_report_dir
                relative_path = file_path.relative_to(html_report_dir)
                # Attach with preserved hierarchy: llvm_coverage_html_report/...
                files_to_attach.append(f"llvm_coverage_html_report/{relative_path}")


    Result.create_from(
        results=[result],
        files=files_to_attach,
        info="LLVM Coverage Merge Job Completed",
    ).complete_job(disable_attached_files_sorting=True)
