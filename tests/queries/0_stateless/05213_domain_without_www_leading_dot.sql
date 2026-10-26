-- Stripping "www." must not reintroduce a leading dot that checkAndReturnHost already rejects on the
-- raw host. "www..com" passes host validation (the dot after "www" is not the leading one), but
-- removing "www." leaves ".com", which is exactly the shape that's supposed to be rejected.
SELECT domainWithoutWWW('http://www..com/') FORMAT CSV;
SELECT domainWithoutWWWRFC('http://www..com/') FORMAT CSV;

-- Control: a normal "www." strip still works.
SELECT domainWithoutWWW('http://www.example.com/') FORMAT CSV;
SELECT domainWithoutWWWRFC('http://www.example.com/') FORMAT CSV;
