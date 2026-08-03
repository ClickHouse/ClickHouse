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

-- 10. A negative and a UInt64-range value inside the SAME container in ONE row. On master both rows
-- inferred an unsigned element type, and the array row then failed to read its own bytes, because the
-- element merge could not see which element came from a negative literal. Passing the provenance into
-- that merge (the same argument group 12's post-transform collapse needs) fixes the array row; the
-- tuple row keeps every element type, so it never needed the provenance and is unchanged.
SELECT 'group 10: an intra-container merge sees the negative marking';
-- c=[-1,18446744073709551615]
DESC format(TSKV, unhex('633D5B2D312C31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B2D312C31383434363734343037333730393535313631355D0A'));
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
-- An array whose elements are not all the same type reaches a second, later collapse: the element
-- types are transformed first, and only then, if the transformation made them equal, is the last
-- element's object kept. A NULL element is what gets the array there, because replacing the Nothing
-- is what makes the rest equal. Both element orders, then a control with no negative element.
-- c=[NULL,-1,1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B4E554C4C2C2D312C315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B4E554C4C2C2D312C315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- c=[NULL,1,-1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B4E554C4C2C312C2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B4E554C4C2C312C2D315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- c=[NULL,1,2] / c=[18446744073709551615] - no negative element, so the widening must still happen
DESC format(TSKV, unhex('633D5B4E554C4C2C312C325D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B4E554C4C2C312C325D0A633D5B31383434363734343037333730393535313631355D0A'));

-- 13. A map with several equal key types, or several equal value types, keeps only the last one and
-- discards the rest, so a negative marking recorded on a discarded object was lost, exactly like the
-- array case in group 12. Only the orders that drop the marked object are affected, which is why the
-- negative-last row below is a control and not a carrier.
SELECT 'group 13: the negative marking survives the map key and value collapse';
-- m={-1:1,1:1} / m={18446744073709551615:1} - two equal keys, only the last object survives
DESC format(TSKV, unhex('6D3D7B2D313A312C313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A312C313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
-- m={'a':-1,'b':1} / m={'a':18446744073709551615} - the same for two equal VALUE types
DESC format(TSKV, unhex('6D3D7B2761273A2D312C2762273A317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A2D312C2762273A317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
-- m={1:1,-1:1} / m={18446744073709551615:1} - the negative key LAST, correct before this change too
DESC format(TSKV, unhex('6D3D7B313A312C2D313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B313A312C2D313A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
-- Carrying the marking across the collapse must not mark anything that was not negative:
-- m={1:1,2:1} / m={18446744073709551615:1} - no negative key, so the widening must still happen
DESC format(TSKV, unhex('6D3D7B313A312C323A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B313A312C323A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
-- m={'a':1,'b':1} / m={'a':18446744073709551615} - the same for values
DESC format(TSKV, unhex('6D3D7B2761273A312C2762273A317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A312C2762273A317D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));

-- 13b. All the map rows above spread the negative and the UInt64-range literal across two ROWS, which
-- merges them through transformTypesIfNeeded. A single row holding both goes through a different
-- statement instead: the element types of that one map are unified inside tryInferMapOrObject, which
-- must consult the same provenance or the negative key is widened and the row cannot be read back.
SELECT 'group 13b: the negative marking survives unifying one row''s own map elements';
-- m={-1:1,18446744073709551615:1} - both keys in ONE row, so the KEY list is unified intra-row
DESC format(TSKV, unhex('6D3D7B2D313A312C31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A312C31383434363734343037333730393535313631353A317D0A'));
-- m={'a':-1,'b':18446744073709551615} - the same for the VALUE list
DESC format(TSKV, unhex('6D3D7B2761273A2D312C2762273A31383434363734343037333730393535313631357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A2D312C2762273A31383434363734343037333730393535313631357D0A'));
-- c=[{-1:1,18446744073709551615:1}] - nested one level, since the unification is inside the recursion
DESC format(TSKV, unhex('633D5B7B2D313A312C31383434363734343037333730393535313631353A317D5D0A'));
SELECT * FROM format(TSKV, unhex('633D5B7B2D313A312C31383434363734343037333730393535313631353A317D5D0A'));
-- Controls, unchanged by consulting the provenance here:
-- m={1:1,2:1} - two non-negative keys already unify as Int64 without widening
DESC format(TSKV, unhex('6D3D7B313A312C323A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B313A312C323A317D0A'));
-- m={-1:1,-2:1} - two negative keys must still unify as Int64
DESC format(TSKV, unhex('6D3D7B2D313A312C2D323A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D313A312C2D323A317D0A'));

-- 13c. What decides whether an integer literal can be read back as UInt64 is the sign it is WRITTEN
-- with, not the value it parses to. A signed zero parses to a value that is not negative, and an
-- explicit plus is accepted by the inference parser but not by the deserializer that reads the value,
-- so both used to look unsigned to the merge and be widened, and then the row could not be read.
SELECT 'group 13c: a signed literal is not widened even when its value is not negative';
-- x=-0 / x=18446744073709551615 - the value is zero, so a value-based test misses the sign
DESC format(TSKV, unhex('783D2D300A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D300A783D31383434363734343037333730393535313631350A'));
-- x=+1 / x=18446744073709551615 - readIntTextUnsafe stops before a '+', so the value path cannot read it
DESC format(TSKV, unhex('783D2B310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2B310A783D31383434363734343037333730393535313631350A'));
-- c=[-0] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B2D305D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B2D305D0A633D5B31383434363734343037333730393535313631355D0A'));
-- m={-0:1} / m={18446744073709551615:1}
DESC format(TSKV, unhex('6D3D7B2D303A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2D303A317D0A6D3D7B31383434363734343037333730393535313631353A317D0A'));
-- m={'a':-0} / m={'a':18446744073709551615}
DESC format(TSKV, unhex('6D3D7B2761273A2D307D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
SELECT * FROM format(TSKV, unhex('6D3D7B2761273A2D307D0A6D3D7B2761273A31383434363734343037333730393535313631357D0A'));
-- c=(-0,1) / c=(18446744073709551615,1)
DESC format(TSKV, unhex('633D282D302C31290A633D2831383434363734343037333730393535313631352C31290A'));
SELECT * FROM format(TSKV, unhex('633D282D302C31290A633D2831383434363734343037333730393535313631352C31290A'));
-- Controls that must keep widening, because their literals carry no sign:
-- x=0 / x=18446744073709551615
DESC format(TSKV, unhex('783D300A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D300A783D31383434363734343037333730393535313631350A'));
-- c=[1] / c=[18446744073709551615]
DESC format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
SELECT * FROM format(TSKV, unhex('633D5B315D0A633D5B31383434363734343037333730393535313631355D0A'));
-- x=-1 / x=18446744073709551615 - a plainly negative literal must still decline
DESC format(TSKV, unhex('783D2D310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D31383434363734343037333730393535313631350A'));
-- x=-0.0 / x=18446744073709551615 - a signed zero written as a float is a Float64, which reads back
DESC format(TSKV, unhex('783D2D302E300A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D302E300A783D31383434363734343037333730393535313631350A'));
-- x=-0 alone - with nothing to merge with, the sign only has to survive the read
DESC format(TSKV, unhex('783D2D300A'));
SELECT * FROM format(TSKV, unhex('783D2D300A'));

-- 13d. Which signs make a literal unreadable is a property of the READER, not of the text, so the two
-- halves of the test above cannot both be applied everywhere. A '-' is refused by every integer reader.
-- A '+' is refused only by readIntTextUnsafe, which the escaped and raw value readers use; readIntText,
-- which the JSON value reader uses, reads '+1' as 1. So the same '+1' token must keep declining the
-- widening under TSKV and keep allowing it under JSON, which is what these rows assert side by side.
-- JSON reaches this inference code from a quoted string only when
-- input_format_json_try_infer_numbers_from_strings is on. Its default is 0, so the setting is written
-- on the individual queries below: it is what makes the path reachable at all.
SELECT 'group 13d: an explicit plus is only unreadable where the reader refuses one';
-- JSON {"a":"+1"} / {"a":"18446744073709551615"} - readIntText accepts the '+', so this must widen
DESC format(JSONEachRow, unhex('7B2261223A222B31227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
SELECT * FROM format(JSONEachRow, unhex('7B2261223A222B31227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
-- JSON {"a":"-1"} / the same UInt64-range row - a '-' is refused by every reader, so this must decline
DESC format(JSONEachRow, unhex('7B2261223A222D31227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
SELECT * FROM format(JSONEachRow, unhex('7B2261223A222D31227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1; -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
-- JSON {"a":"1"} / the same row - no sign at all, so nothing is recorded and it widens
DESC format(JSONEachRow, unhex('7B2261223A2231227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
SELECT * FROM format(JSONEachRow, unhex('7B2261223A2231227D0A7B2261223A223138343436373434303733373039353531363135227D0A')) SETTINGS input_format_json_try_infer_numbers_from_strings = 1;
-- TSKV x=+1 / x=18446744073709551615 - the SAME token, read by readIntTextUnsafe, must NOT widen
DESC format(TSKV, unhex('783D2B310A783D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2B310A783D31383434363734343037333730393535313631350A'));

-- 13e. Declining to widen only settles what two rows agree on. A single row carrying a '+' still has to
-- be readable on its own, and no merge happens there to consult, so the type it infers must be one the
-- value reader accepts. readIntTextUnsafe stops before the '+' and leaves it in the buffer, so an
-- integer type cannot be read back at all: the field parses as zero and the leftover '+' is reported as
-- garbage after the field. Such a field stays a String, exactly as an integer with a leading zero does.
SELECT 'group 13e: a single field the value reader refuses stays a String';
-- x=+1 alone - inference accepts the '+', the value reader does not, so this must not infer an integer
DESC format(TSKV, unhex('783D2B310A'));
SELECT * FROM format(TSKV, unhex('783D2B310A'));
-- x=+18446744073709551615 alone - the same, reached through the unsigned branch of the number parser
DESC format(TSKV, unhex('783D2B31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2B31383434363734343037333730393535313631350A'));
-- x=+0 alone - a signed zero is refused for the sign, not for the zero
DESC format(TSKV, unhex('783D2B300A'));
SELECT * FROM format(TSKV, unhex('783D2B300A'));
-- x=+00 alone - both the sign and the leading zero would make it unreadable
DESC format(TSKV, unhex('783D2B30300A'));
SELECT * FROM format(TSKV, unhex('783D2B30300A'));
-- Controls. A float reader does consume a leading '+', so floats must keep inferring Float64:
-- x=+1.5 alone
DESC format(TSKV, unhex('783D2B312E350A'));
SELECT * FROM format(TSKV, unhex('783D2B312E350A'));
-- x=+0.0 alone
DESC format(TSKV, unhex('783D2B302E300A'));
SELECT * FROM format(TSKV, unhex('783D2B302E300A'));
-- x=-1 alone - the integer reader does consume a '-', so a negative literal needs no fallback
DESC format(TSKV, unhex('783D2D310A'));
SELECT * FROM format(TSKV, unhex('783D2D310A'));
-- x=1 alone - no sign at all
DESC format(TSKV, unhex('783D310A'));
SELECT * FROM format(TSKV, unhex('783D310A'));

-- 14. The marking is keyed on the type object address, and one column's dropped type object can be
-- freed while another column is still being inferred. If the address is then reused, the marking is
-- read as belonging to whatever type landed there, so an unrelated column declines a widening it
-- should perform. That makes the answer depend on the row order, which is what these rows assert:
-- both orders must give the same type for y, so neither is a carrier once the address is kept alive.
SELECT 'group 14: one column''s marking does not leak into another column';
-- x=1 / x=-1 / y=1 / y=18446744073709551615 - y has no negative value, so it must still widen
DESC format(TSKV, unhex('783D310A783D2D310A793D310A793D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D2D310A793D310A793D31383434363734343037333730393535313631350A'));
-- x=-1 / x=1 / y=1 / y=18446744073709551615 - the opposite order must agree about y
DESC format(TSKV, unhex('783D2D310A783D310A793D310A793D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D2D310A783D310A793D310A793D31383434363734343037333730393535313631350A'));
-- x=1 / x=-1 / y=-1 / y=18446744073709551615 - y IS negative here, so it must still decline
DESC format(TSKV, unhex('783D310A783D2D310A793D2D310A793D31383434363734343037333730393535313631350A'));
SELECT * FROM format(TSKV, unhex('783D310A783D2D310A793D2D310A793D31383434363734343037333730393535313631350A'));

-- 15. The same inference code serves the other formats that read fields by an escaping rule and keep
-- a provenance set, so they get the same collapse and the same order-independence. Each one carries its
-- own reader override and its own provenance set, so each is asserted separately here. Template is the
-- one that cannot be: its input format takes the row format only as a file path
-- (TemplateRowInputFormat.cpp reads template_settings.row_format; the inline
-- format_template_row_format setting is read by the output format alone), so it needs a schema file and
-- lives in 04654, which is a shell test and can write one.
SELECT 'group 15: a sibling escaped-rule format is order-independent too';
-- 1 / -1 / 18446744073709551615
DESC format(CustomSeparated, unhex('310A2D310A31383434363734343037333730393535313631350A'));
SELECT * FROM format(CustomSeparated, unhex('310A2D310A31383434363734343037333730393535313631350A'));
-- -1 / 1 / 18446744073709551615 - the opposite order must agree
DESC format(CustomSeparated, unhex('2D310A310A31383434363734343037333730393535313631350A'));
SELECT * FROM format(CustomSeparated, unhex('2D310A310A31383434363734343037333730393535313631350A'));
-- 1 / 2 / 18446744073709551615 - no negative value, so the widening must still happen
DESC format(CustomSeparated, unhex('310A320A31383434363734343037333730393535313631350A'));
SELECT * FROM format(CustomSeparated, unhex('310A320A31383434363734343037333730393535313631350A'));
-- +1 alone - the single-row fallback of group 13e applies here too, through the shared escaping rule
DESC format(CustomSeparated, unhex('2B310A'));
SELECT * FROM format(CustomSeparated, unhex('2B310A'));
-- +1.5 alone - and floats keep their type here too
DESC format(CustomSeparated, unhex('2B312E350A'));
SELECT * FROM format(CustomSeparated, unhex('2B312E350A'));
-- -1 alone - the negative control needs no fallback
DESC format(CustomSeparated, unhex('2D310A'));
SELECT * FROM format(CustomSeparated, unhex('2D310A'));

-- The same three inputs through Regexp, whose escaping rule is a separate setting and whose reader
-- keeps its own provenance set. The settings are per statement so the other groups are unaffected.
-- Reading the values back is the point: without the fix the negative row silently reads as 0.
SELECT 'group 15b: Regexp is order-independent too';
-- 1 / -1 / 18446744073709551615
DESC format(Regexp, unhex('310A2D310A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('310A2D310A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
-- -1 / 1 / 18446744073709551615 - the opposite order must agree
DESC format(Regexp, unhex('2D310A310A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('2D310A310A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
-- 1 / 2 / 18446744073709551615 - no negative value, so the widening must still happen
DESC format(Regexp, unhex('310A320A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('310A320A31383434363734343037333730393535313631350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
-- +1 alone - the single-row fallback of group 13e applies here too
DESC format(Regexp, unhex('2B310A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('2B310A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
-- +1.5 alone - and floats keep their type here too
DESC format(Regexp, unhex('2B312E350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('2B312E350A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
-- -1 alone - the negative control needs no fallback
DESC format(Regexp, unhex('2D310A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';
SELECT * FROM format(Regexp, unhex('2D310A')) SETTINGS format_regexp = '^(.+)$', format_regexp_escaping_rule = 'Escaped';

-- The round trip through the writer, asserted without hand-written bytes: what TSKV emits for a
-- negative map key must infer a type that reads back. This is the reason group 11 is not a
-- hand-crafted-input-only concern.
SELECT 'group 11 round trip: TSKV output for a negative map key reads back';
-- The alias must come from a FROM-clause subquery, and the whole document from one scalar subquery:
-- an inline `expr AS m` names the TSKV key after the expression text without the analyzer, and
-- concatenating two subqueries in the argument is not a constant expression there.
SELECT (SELECT formatRow('TSKV', m) FROM (SELECT map(-1, 1) AS m)) = 'm={-1:1}\n';
DESC format(TSKV, (SELECT (SELECT formatRow('TSKV', m) FROM (SELECT map(-1, 1) AS m)) || (SELECT formatRow('TSKV', m) FROM (SELECT map(18446744073709551615, 1) AS m))));
SELECT * FROM format(TSKV, (SELECT (SELECT formatRow('TSKV', m) FROM (SELECT map(-1, 1) AS m)) || (SELECT formatRow('TSKV', m) FROM (SELECT map(18446744073709551615, 1) AS m))));

-- The same for a signed zero, which is likewise what the writer emits rather than a hand-written shape:
-- a negative Float64 zero is written without its fractional part, so the field reads back as the integer
-- literal -0. Group 13c is therefore not a hand-crafted-input-only concern either.
SELECT 'group 13c round trip: TSKV output for a negative zero reads back';
SELECT (SELECT formatRow('TSKV', x) FROM (SELECT -0.0::Float64 AS x)) = 'x=-0\n';
DESC format(TSKV, (SELECT (SELECT formatRow('TSKV', x) FROM (SELECT -0.0::Float64 AS x)) || (SELECT formatRow('TSKV', x) FROM (SELECT 18446744073709551615::UInt64 AS x))));
SELECT * FROM format(TSKV, (SELECT (SELECT formatRow('TSKV', x) FROM (SELECT -0.0::Float64 AS x)) || (SELECT formatRow('TSKV', x) FROM (SELECT 18446744073709551615::UInt64 AS x))));
