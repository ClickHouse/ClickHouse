-- TSKV schema inference reads the raw field bytes, the same bytes its value path parses. Every input
-- below is written as unhex so the exact wire bytes are unambiguous: a backslash in a SQL or shell
-- string literal would be re-interpreted before reaching the format.

-- 1. A decoded escape used to make inference propose a type the value path cannot read: DESC succeeded
-- and SELECT hard-errored on the same bytes. All of these are Code 130/27/563 on master.
SELECT 'group 1: an escaped value inside a compound or a date now infers String and reads back';
-- c=[0\x2E5]
DESC format(TSKV, unhex('633D5B305C783245355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B305C783245355D0A'));
-- c=[1\x2E5]
DESC format(TSKV, unhex('633D5B315C783245355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315C783245355D0A'));
-- c=[1\x30]
DESC format(TSKV, unhex('633D5B315C7833305D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315C7833305D0A'));
-- t=(1,0\x2E5)
DESC format(TSKV, unhex('743D28312C305C78324535290A'));
SELECT * FROM format(TSKV, unhex('743D28312C305C78324535290A'));
-- m={'a':0\x2E5}
DESC format(TSKV, unhex('6D3D7B2761273A305C783245357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A305C783245357D0A'));
-- c=[[1\x2E5]]
DESC format(TSKV, unhex('633D5B5B315C783245355D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B5B315C783245355D5D0A'));
-- x=2020-01-0\x31
DESC format(TSKV, unhex('783D323032302D30312D305C7833310A'));
SELECT * FROM format(TSKV, unhex('783D323032302D30312D305C7833310A'));
-- x=2020-01-01 00:00:0\x31
DESC format(TSKV, unhex('783D323032302D30312D30312030303A30303A305C7833310A'));
SELECT * FROM format(TSKV, unhex('783D323032302D30312D30312030303A30303A305C7833310A'));

-- 2. Escaped scalars, asserted so a future simplification cannot silently reopen them.
SELECT 'group 2: escaped scalars';
-- x=1\x2E5
DESC format(TSKV, unhex('783D315C783245350A'));
SELECT * FROM format(TSKV, unhex('783D315C783245350A'));
-- x=1\x30
DESC format(TSKV, unhex('783D315C7833300A'));
SELECT * FROM format(TSKV, unhex('783D315C7833300A'));
-- x=tru\x65
DESC format(TSKV, unhex('783D7472755C7836350A'));
SELECT * FROM format(TSKV, unhex('783D7472755C7836350A'));
-- x=fals\x65
DESC format(TSKV, unhex('783D66616C735C7836350A'));
SELECT * FROM format(TSKV, unhex('783D66616C735C7836350A'));

-- 3. Data without escapes must infer exactly what it did before. These pin that reading the raw bytes
-- does not turn ordinary values into String.
SELECT 'group 3: escape-free controls keep their types';
-- x=1.5
DESC format(TSKV, unhex('783D312E350A'));
SELECT * FROM format(TSKV, unhex('783D312E350A'));
-- x=007
DESC format(TSKV, unhex('783D3030370A'));
SELECT * FROM format(TSKV, unhex('783D3030370A'));
-- c=[1]
DESC format(TSKV, unhex('633D5B315D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315D0A'));
-- x=2020-01-01
DESC format(TSKV, unhex('783D323032302D30312D30310A'));
SELECT * FROM format(TSKV, unhex('783D323032302D30312D30310A'));

-- 4. An escape inside a QUOTED element is consumed by that element's own reader, so the compound type is
-- correct and readable. These must keep their compound type: they are what distinguishes reading the raw
-- bytes from the coarser rule "an escape anywhere inside a compound means String".
SELECT 'group 4: an escape inside a quoted element keeps the compound type';
-- c=['a\x2Eb']
DESC format(TSKV, unhex('633D5B27615C78324562275D0A'));
SELECT * FROM format(TSKV, unhex('633D5B27615C78324562275D0A'));
-- t=(1,'0\x2E5')
DESC format(TSKV, unhex('743D28312C27305C7832453527290A'));
SELECT * FROM format(TSKV, unhex('743D28312C27305C7832453527290A'));
-- m={'k':'a\x2Eb'}
DESC format(TSKV, unhex('6D3D7B276B273A27615C78324562277D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B276B273A27615C78324562277D0A'));
-- c=[['a\x2Eb']]
DESC format(TSKV, unhex('633D5B5B27615C78324562275D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B5B27615C78324562275D5D0A'));
-- m={'a\x2Eb':1}
DESC format(TSKV, unhex('6D3D7B27615C78324562273A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B27615C78324562273A317D0A'));
-- x='a\x2Eb'
DESC format(TSKV, unhex('783D27615C78324562270A'));
SELECT * FROM format(TSKV, unhex('783D27615C78324562270A'));

-- 5. TSKV now merges types across rows the way the Escaped rule specifies, like every sibling format.
-- The first shape uses an escape-free null representation so the field really is a null and reaches the
-- merge; on master it answers Nullable(String), because the bare supertype of Nullable(Nothing) and an
-- Array is nothing at all and the format's String default wins.
SELECT 'group 5: type merging across rows';
-- x=NULL / x=[2]
DESC format(TSKV, unhex('783D4E554C4C0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('783D4E554C4C0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
-- x=1 / x=1.5
DESC format(TSKV, unhex('783D310A783D312E350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D312E350A'));
-- x=1 / x=18446744073709551615
DESC format(TSKV, unhex('783D310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D31383434363734343037333730393535313631350A'));
-- c=[1] / c=[1.5]
DESC format(TSKV, unhex('633D5B315D0A633D5B312E355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315D0A633D5B312E355D0A'));
-- x=\N / x=[2] / x=[3] at the default null representation. This shape passes on master too, but only
-- because the decoding reader turned \N into the empty string and the merge was skipped entirely.
DESC format(TSKV, unhex('783D5C4E0A783D5B325D0A783D5B335D0A'));
SELECT * FROM format(TSKV, unhex('783D5C4E0A783D5B325D0A783D5B335D0A'));

-- 6. A doubled backslash decodes to the two characters of the default null representation, so on master
-- the field became a null, the merge was skipped and the next row's Int64 won - and then the read failed
-- on the raw bytes. Reading the raw bytes makes it an ordinary string in both rows.
SELECT 'group 6: a doubled backslash is not a null';
-- x=\\N
DESC format(TSKV, unhex('783D5C5C4E0A'));
SELECT * FROM format(TSKV, unhex('783D5C5C4E0A'));
-- x=\\N / x=1
DESC format(TSKV, unhex('783D5C5C4E0A783D310A'));
SELECT * FROM format(TSKV, unhex('783D5C5C4E0A783D310A'));

-- 7. A truncated escape sequence stays unreadable; only the stage of the failure moves, from inference to
-- the read. Nothing malformed becomes acceptable that was not already.
SELECT 'group 7: truncated escapes stay unreadable';
-- x=1\xA - the newline is consumed as a hex digit, so the value runs past the end of the input
DESC format(TSKV, unhex('783D315C78410A'));
SELECT * FROM format(TSKV, unhex('783D315C78410A')); -- { serverError CANNOT_READ_ALL_DATA }
-- x=1\x
DESC format(TSKV, unhex('783D315C780A'));
SELECT * FROM format(TSKV, unhex('783D315C780A')); -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
-- x=1\ is rejected at inference time here and on master alike
DESC format(TSKV, unhex('783D315C0A')); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
