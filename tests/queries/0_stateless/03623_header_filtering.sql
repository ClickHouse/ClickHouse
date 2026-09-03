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

-- A whitespace-padded 'R ange' normalizes to the banned 'Range' header, so it must be rejected
-- (the Range ban is applied to the normalized, case-insensitive name).
SELECT * FROM url(
    'http://google.com',
    'RawBLOB',
    'data String',
    headers('R ange' = 'bytes=0-1')
); -- {serverError BAD_ARGUMENTS}