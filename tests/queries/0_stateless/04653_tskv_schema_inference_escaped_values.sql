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

-- 8. Merging a negative integer with a value that only fits UInt64 must not widen the negative one:
-- UInt64 is inferred only on Int64 overflow, so the pair has no common integer type and the format's
-- String default wins. The provenance of a negative literal is recorded during inference and read back
-- during the merge, which is why the nested shapes below are covered by the same mechanism as the scalar.
SELECT 'group 8: a negative integer is not widened to UInt64';
-- x=-1 / x=18446744073709551615
DESC format(TSKV, unhex('783D2D310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D31383434363734343037333730393535313631350A'));
-- c=[-1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- c=[[-1]] / c=[[18446744073709551615]]
DESC format(TSKV, unhex('633D5B5B2D315D5D0A633D5B5B31383434363734343037333730393535313631355D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B5B2D315D5D0A633D5B5B31383434363734343037333730393535313631355D5D0A'));
-- c=(-1,1) / c=(18446744073709551615,1)
DESC format(TSKV, unhex('633D282D312C31290A633D2831383434363734343037333730393535313631352C31290A'));
SELECT * FROM format(TSKV, unhex('633D282D312C31290A633D2831383434363734343037333730393535313631352C31290A'));
-- x=-1 / x=5 / x=18446744073709551615 - the non-negative row in the middle is still widened
DESC format(TSKV, unhex('783D2D310A783D350A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D350A783D31383434363734343037333730393535313631350A'));

-- 9. The widening that IS correct must survive: a non-negative Int64 still becomes UInt64, integers still
-- become floats, and without a UInt64 in the picture nothing widens at all.
SELECT 'group 9: correct widening is preserved';
-- c=[1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- x=-1 / x=1.5
DESC format(TSKV, unhex('783D2D310A783D312E350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D312E350A'));
-- x=-1 / x=-2
DESC format(TSKV, unhex('783D2D310A783D2D320A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D2D320A'));

-- 10. PRE-EXISTING, unchanged by this PR and asserted only so a later change cannot move it unnoticed:
-- a negative and a UInt64-range value inside the SAME container in ONE row still infer an unsigned
-- element type, because the element merge inside a container does not carry the provenance. Both rows
-- below behave identically on master.
SELECT 'group 10: intra-container merge is unchanged (pre-existing)';
-- c=[-1,18446744073709551615]
DESC format(TSKV, unhex('633D5B2D312C31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B2D312C31383434363734343037333730393535313631355D0A')); -- { serverError CANNOT_READ_ARRAY_FROM_TEXT }
-- c=(-1,18446744073709551615)
DESC format(TSKV, unhex('633D282D312C3138343436373434303733373039353531363135290A'));
SELECT * FROM format(TSKV, unhex('633D282D312C3138343436373434303733373039353531363135290A'));

-- 11. A negative integer used as a MAP KEY is not widened either. Inference records that an Int64 came
-- from a negative literal for map values but used to discard that for map keys, while the merge does
-- descend into key types, so a negative key looked non-negative and was widened to UInt64.
-- Unlike every escape carrier above, this shape is what ClickHouse's own TSKV writer emits:
-- SELECT map(-1, 1) AS m FORMAT TSKV produces exactly the first row below.
SELECT 'group 11: a negative map key is not widened to UInt64';
-- m={-1:1} / m={18446744073709551615:1} - both rows are also byte-for-byte what the TSKV writer emits
DESC format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
-- c=[{-1:1}] / c=[{18446744073709551615:1}]
DESC format(TSKV, unhex('633D5B7B2D313A317D5D0A633D5B7B31383434363734343037333730393535313631353A317D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B7B2D313A317D5D0A633D5B7B31383434363734343037333730393535313631353A317D5D0A'));
-- c=[[{-1:1}]] / c=[[{18446744073709551615:1}]]
DESC format(TSKV, unhex('633D5B5B7B2D313A317D5D5D0A633D5B5B7B31383434363734343037333730393535313631353A317D5D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B5B7B2D313A317D5D5D0A633D5B5B7B31383434363734343037333730393535313631353A317D5D5D0A'));
-- t=(1,{-1:1}) / t=(1,{18446744073709551615:1})
DESC format(TSKV, unhex('743D28312C7B2D313A317D290A743D28312C7B31383434363734343037333730393535313631353A317D290A'));
SELECT * FROM format(TSKV, unhex('743D28312C7B2D313A317D290A743D28312C7B31383434363734343037333730393535313631353A317D290A'));
-- m={1:{-1:1}} / m={1:{18446744073709551615:1}}
DESC format(TSKV, unhex('6D3D7B313A7B2D313A317D7D0A6D3D7B313A7B31383434363734343037333730393535313631353A317D7D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B313A7B2D313A317D7D0A6D3D7B313A7B31383434363734343037333730393535313631353A317D7D0A'));
-- m={'a':{-1:1}} / m={'a':{18446744073709551615:1}}
DESC format(TSKV, unhex('6D3D7B2761273A7B2D313A317D7D0A6D3D7B2761273A7B31383434363734343037333730393535313631353A317D7D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A7B2D313A317D7D0A6D3D7B2761273A7B31383434363734343037333730393535313631353A317D7D0A'));
-- c=[(1,{-1:1})] / c=[(1,{18446744073709551615:1})]
DESC format(TSKV, unhex('633D5B28312C7B2D313A317D295D0A633D5B28312C7B31383434363734343037333730393535313631353A317D295D0A'));
SELECT * FROM format(TSKV, unhex('633D5B28312C7B2D313A317D295D0A633D5B28312C7B31383434363734343037333730393535313631353A317D295D0A'));
-- m={-1:1} / m={2.5:1} - a negative key still becomes Float64, which reads back
DESC format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B322E353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B322E353A317D0A'));
-- The provenance must reach ONLY map keys inferred from a negative literal:
-- m={-1:1} / m={-2:1} - two negative keys still unify as Int64
DESC format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B2D323A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A317D0A6D3D7B2D323A317D0A'));
-- m={'a':-1} / m={'a':18446744073709551615} - the map VALUE path was already correct
DESC format(TSKV, unhex('6D3D7B2761273A2D317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A2D317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
-- m={'a':1} / m={'b':1} - string keys are unaffected
DESC format(TSKV, unhex('6D3D7B2761273A317D0A6D3D7B2762273A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A317D0A6D3D7B2762273A317D0A'));
-- m={1:1} / m={18446744073709551615:1} - a NON-negative key must still widen, which reads back
DESC format(TSKV, unhex('6D3D7B313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));

-- 12. The provenance also has to survive being collapsed. When two rows infer the same type, the
-- retained type object is the first row's and the second row's is dropped; when all elements of an
-- array have the same type, only the last element's object is kept. Either way the dropped object
-- carried the negative marking, so a later UInt64 widened what looked like an unmarked Int64. Both
-- orders are asserted, because the array case depends on which element is dropped.
SELECT 'group 12: the negative marking survives collapsing equal types';
-- x=1 / x=-1 / x=18446744073709551615 - rows 1 and 2 are both Int64, so row 2's marking was dropped
DESC format(TSKV, unhex('783D310A783D2D310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D2D310A783D31383434363734343037333730393535313631350A'));
-- c=[-1,1] / c=[18446744073709551615] - only the LAST element's type object is kept
DESC format(TSKV, unhex('633D5B2D312C315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B2D312C315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- x=-1 / x=5 / x=18446744073709551615 - the opposite row order
DESC format(TSKV, unhex('783D2D310A783D350A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D350A783D31383434363734343037333730393535313631350A'));
-- c=[1,-1] / c=[18446744073709551615] - the opposite element order, correct before this change too
DESC format(TSKV, unhex('633D5B312C2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B312C2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- Carrying the marking over must not mark anything that was not negative:
-- x=1 / x=18446744073709551615
DESC format(TSKV, unhex('783D310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D31383434363734343037333730393535313631350A'));
-- c=[1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- x=-1 / x=1.5
DESC format(TSKV, unhex('783D2D310A783D312E350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D312E350A'));
-- x=-1 / x=-2
DESC format(TSKV, unhex('783D2D310A783D2D320A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D2D320A'));

-- The round trip through the writer, asserted without hand-written bytes: what TSKV emits for a
-- negative map key must infer a type that reads back. This is the reason group 11 is not a
-- hand-crafted-input-only concern.
SELECT 'group 11 round trip: TSKV output for a negative map key reads back';
SELECT formatRow('TSKV', map(-1, 1) AS m) = 'm={-1:1}\n';
DESC format(TSKV, ((SELECT formatRow('TSKV', map(-1, 1) AS m)) || (SELECT formatRow('TSKV', map(18446744073709551615, 1) AS m))));
SELECT * FROM format(TSKV, ((SELECT formatRow('TSKV', map(-1, 1) AS m)) || (SELECT formatRow('TSKV', map(18446744073709551615, 1) AS m))));
