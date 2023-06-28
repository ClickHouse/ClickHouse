SET max_threads='-1'; -- { serverError 72 }
SET max_final_threads='-1'; -- { serverError 72 }
SET max_download_threads='-1'; -- { serverError 72 }

SET max_threads=-1; -- { serverError 70 }
SET max_final_threads=-1; -- { serverError 70 }
SET max_download_threads=-1; -- { serverError 70 }

SET max_threads='nan'; -- { serverError 72 }
SET max_final_threads='nan'; -- { serverError 72 }
SET max_download_threads='nan'; -- { serverError 72 }

SET max_threads='inf'; -- { serverError 72 }
SET max_final_threads='inf'; -- { serverError 72 }
SET max_download_threads='inf'; -- { serverError 72 }

SET max_threads='-0+1'; -- { serverError 72 }
SET max_final_threads='-0+1'; -- { serverError 72 }
SET max_download_threads='-0+1'; -- { serverError 72 }

SET max_threads='++'; -- { serverError 72 }
SET max_final_threads='++'; -- { serverError 72 }
SET max_download_threads='++'; -- { serverError 72 }
