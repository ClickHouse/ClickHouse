from ci.praktika.result import Result
from ci.praktika.utils import Utils

if __name__ == "__main__":
    result = Result.from_commands_run(
        name="Merge LLVM Coverage",
        command=["bash ci/jobs/merge_llvm_coverage.sh"],
    )
    
    attached_files = []
    
    # Compress coverage artifacts
    coverage_files = [
        "./ci/tmp/merged.profdata",
        "./ci/tmp/llvm_coverage_html_report",
    ]
    attached_files.append(
        Utils.compress_files_gz(coverage_files, "./ci/tmp/llvm_coverage_html_report.tar.gz")
    )
    
    Result.create_from(
        results=[result],
        files=attached_files,
        message="LLVM Coverage Merge Job Completed",
    ).complete_job()
