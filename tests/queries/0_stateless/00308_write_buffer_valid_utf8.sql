SET output_format_write_statistics = 0;
SELECT concat('Hello, ', unhex('a0'), ' World') AS s1, concat('Hello, ', unhex('a0')) AS s2, concat(unhex('a0'), ' World') AS s3 FORMAT JSONCompact;
