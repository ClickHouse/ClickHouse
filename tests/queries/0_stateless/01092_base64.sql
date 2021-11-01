-- Tags: no-fasttest

-- This query reproduces a bug in TurboBase64 library.
select distinct base64Encode(materialize('LG Optimus')) from numbers(100);
