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

-- Schema inference must not send a 'Range' header (it would read a partial-content response). A
-- whitespace-padded 'R ange' normalizes to 'Range', so it is rejected during inference, before any
-- request. (The read/ATTACH paths keep their existing behaviour, so this ban is inference-only.)
DESCRIBE url(
    'http://google.com',
    headers('R ange' = 'bytes=0-1')
); -- {serverError BAD_ARGUMENTS}