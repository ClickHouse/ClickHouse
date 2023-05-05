-- Tags: no-fasttest

SET send_logs_level = 'fatal';

SELECT base64Encode(val) FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val);

SELECT base64Decode(val) FROM (select arrayJoin(['', 'Zg==', 'Zm8=', 'Zm9v', 'Zm9vYg==', 'Zm9vYmE=', 'Zm9vYmFy']) val);
SELECT base64Decode(base64Encode('foo')) = 'foo', base64Encode(base64Decode('Zm9v')) == 'Zm9v';

SELECT tryBase64Decode('Zm9vYmF=Zm9v');

SELECT base64Encode(val, 'excess argument') FROM (select arrayJoin(['', 'f', 'fo', 'foo', 'foob', 'fooba', 'foobar']) val); -- { serverError 42 }
SELECT base64Decode(val, 'excess argument') FROM (select arrayJoin(['', 'Zg==', 'Zm8=', 'Zm9v', 'Zm9vYg==', 'Zm9vYmE=', 'Zm9vYmFy']) val); -- { serverError 42 }
SELECT tryBase64Decode('Zm9vYmF=Zm9v', 'excess argument'); -- { serverError 42 }

SELECT base64Decode('Zm9vYmF=Zm9v'); -- { serverError 117 }

select base64Encode(toFixedString('foo', 3));
select base64Decode(toFixedString('Zm9v', 4));
