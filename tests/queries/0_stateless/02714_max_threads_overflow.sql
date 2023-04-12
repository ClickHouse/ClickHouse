SET max_threads=-1; -- { serverError 36 }
SET max_final_threads=-1; -- { serverError 36 }
SET max_download_threads=-1; -- { serverError 36 }
