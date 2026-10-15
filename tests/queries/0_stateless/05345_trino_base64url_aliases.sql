-- Tags: no-fasttest
-- no-fasttest because simdutf library is required

-- Trino alias of `base64URLDecode`.
SELECT from_base64url('Pz4'), FROM_BASE64URL('Pz4');
