-- TSKV schema inference reads the raw field bytes, the same bytes its value path parses.
-- Inputs are written as unhex so the wire bytes are exact.

-- 1. An escaped value inside a compound or a date: DESC succeeded and SELECT failed on master.
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

-- 2. Escaped scalars.
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

-- 3. Values without escapes keep their types.
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

-- 4. An escape inside a quoted element is decoded by that element's reader, so the compound type stays.
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

-- 5. Rows are merged the same way as in TSV.
SELECT 'group 5: rows are merged the same way as in TSV';
-- x=\N / x=[2] / x=[3], the shape 02240 pins
DESC format(TSKV, unhex('783D5C4E0A783D5B325D0A783D5B335D0A'));
SELECT * FROM format(TSKV, unhex('783D5C4E0A783D5B325D0A783D5B335D0A'));
-- x=NULL / x=[2] with NULL as the null representation
DESC format(TSKV, unhex('783D4E554C4C0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('783D4E554C4C0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
-- x=[1] / x=NULL with NULL as the null representation
DESC format(TSKV, unhex('783D5B315D0A783D4E554C4C0A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('783D5B315D0A783D4E554C4C0A')) SETTINGS format_tsv_null_representation = 'NULL';
-- m={'a':1} / m=NULL with NULL as the null representation
DESC format(TSKV, unhex('6D3D7B2761273A317D0A6D3D4E554C4C0A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A317D0A6D3D4E554C4C0A')) SETTINGS format_tsv_null_representation = 'NULL';
-- x=NULL / x=(1,2) with NULL as the null representation
DESC format(TSKV, unhex('783D4E554C4C0A783D28312C32290A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('783D4E554C4C0A783D28312C32290A')) SETTINGS format_tsv_null_representation = 'NULL';
-- x=1 / x=1.5
DESC format(TSKV, unhex('783D310A783D312E350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D312E350A'));
-- x=\N / x=[2] with NULL as the null representation, so \N is data
DESC format(TSKV, unhex('783D5C4E0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
SELECT * FROM format(TSKV, unhex('783D5C4E0A783D5B325D0A')) SETTINGS format_tsv_null_representation = 'NULL';
-- x=\N\N / x=[2], data rather than the null representation
DESC format(TSKV, unhex('783D5C4E5C4E0A783D5B325D0A'));
SELECT * FROM format(TSKV, unhex('783D5C4E5C4E0A783D5B325D0A'));

-- 6. A doubled backslash is a string, not the null representation.
SELECT 'group 6: a doubled backslash is not a null';
-- x=\\N
DESC format(TSKV, unhex('783D5C5C4E0A'));
SELECT * FROM format(TSKV, unhex('783D5C5C4E0A'));
-- x=\\N / x=1
DESC format(TSKV, unhex('783D5C5C4E0A783D310A'));
SELECT * FROM format(TSKV, unhex('783D5C5C4E0A783D310A'));

-- 7. A truncated escape stays unreadable; only the stage of the failure moves from inference to the read.
SELECT 'group 7: truncated escapes stay unreadable';
-- x=1\xA, the newline is consumed as a hex digit
DESC format(TSKV, unhex('783D315C78410A'));
SELECT * FROM format(TSKV, unhex('783D315C78410A')); -- { serverError CANNOT_READ_ALL_DATA }
-- x=1\x
DESC format(TSKV, unhex('783D315C780A'));
SELECT * FROM format(TSKV, unhex('783D315C780A')); -- { serverError CANNOT_PARSE_ESCAPE_SEQUENCE }
-- x=1\ is rejected at inference, as on master
DESC format(TSKV, unhex('783D315C0A')); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
