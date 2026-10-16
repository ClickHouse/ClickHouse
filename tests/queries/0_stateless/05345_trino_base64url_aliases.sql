-- Tags: no-fasttest
-- no-fasttest because simdutf library is required

-- Trino aliases of `base64URLEncode` and `base64URLDecode`.
SELECT to_base64url('?>'), from_base64url('Pz4'), TO_BASE64URL('?>');
