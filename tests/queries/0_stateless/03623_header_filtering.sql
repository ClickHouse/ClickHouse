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