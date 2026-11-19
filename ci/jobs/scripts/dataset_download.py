import time
from concurrent.futures import ThreadPoolExecutor

from ci.praktika.utils import Shell


def download_and_extract_datasets(dataset_urls, target_dir, retries=5):
    """Download dataset tarballs in parallel and extract them into target_dir.

    Returns a list of error descriptions, one per dataset that could not be
    downloaded and extracted; an empty list means success. The caller can put
    the errors into Result.info so the failure is visible in the CI report
    without digging through the job log.

    S3 sporadically returns transient errors (e.g. 503 Service Unavailable),
    and wget retries only network-level errors, not HTTP errors, so a single
    503 would otherwise fail the whole job. A failed attempt cannot be resumed
    mid-stream because the body is piped into tar, so rerun the whole pipeline
    on any failure - extraction into target_dir is idempotent.
    """

    def download(url):
        error = ""
        for attempt in range(1, retries + 1):
            exit_code, _, error = Shell.get_res_stdout_stderr(
                f'wget -nv -nd "{url}" -O- | tar --extract -C {target_dir}',
                verbose=True,
            )
            if exit_code == 0:
                return ""
            print(
                f"WARNING: Attempt [{attempt}/{retries}] to download [{url}] failed, err: [{error}]"
            )
            if attempt < retries:
                time.sleep(attempt * 30)
        return f"Failed to download [{url}] after [{retries}] attempts, last error: [{error}]"

    print(f"Download datasets in parallel: [{len(dataset_urls)}]")
    with ThreadPoolExecutor(max_workers=len(dataset_urls)) as executor:
        return [error for error in executor.map(download, dataset_urls) if error]
