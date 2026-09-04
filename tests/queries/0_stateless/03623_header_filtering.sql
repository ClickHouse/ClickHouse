SELECT * FROM url(
    'http://google.com',
    'RawBLOB',
    'data String',
    headers('exact_header'='true')
); -- {serverError BAD_ARGUMENTS}

SELECT * FROM url(
    'http://google.com',
    'RawBLOB',
    'data String',
    headers('exact_header	' = 'true', 'exact_header	' = 'true')
); -- {serverError BAD_ARGUMENTS}

-- A whitespace-padded 'R ange' normalizes to the banned 'Range' header, so it is rejected before
-- any request (the ban is applied to the normalized, case-insensitive name) — on both the read
-- path (explicit structure) and the schema-inference path (no structure).
SELECT * FROM url(
    'http://google.com',
    'RawBLOB',
    'data String',
    headers('R ange' = 'bytes=0-1')
); -- {serverError BAD_ARGUMENTS}

DESCRIBE url(
    'http://google.com',
    headers('R ange' = 'bytes=0-1')
); -- {serverError BAD_ARGUMENTS}

-- A listable wildcard ('*' in the path) delegates to the web backend, which is a different code
-- path; the Range ban must hold there too.
DESCRIBE url(
    'http://localhost:11111/foo*',
    headers('R ange' = 'bytes=0-1')
) SETTINGS allow_experimental_url_wildcard_from_index_pages = 1; -- {serverError BAD_ARGUMENTS}