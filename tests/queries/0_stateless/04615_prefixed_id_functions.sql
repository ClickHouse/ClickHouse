-- The reference table of split semantics: split at the last underscore, never fails.
SELECT id, prefixedIDPrefix(id), prefixedIDBody(id), splitPrefixedID(id) FROM (SELECT arrayJoin([
    'user_NffrFeUfNV2Hib',
    'ch_test_51TpZvW',
    'abc123',
    '',
    'user_',
    '_abc']) id);

-- The scalar functions must agree with the tuple function.
SELECT sum((prefixedIDPrefix(id), prefixedIDBody(id)) != splitPrefixedID(id)) FROM (SELECT arrayJoin([
    'user_NffrFeUfNV2Hib', 'ch_test_51TpZvW', 'abc123', '', 'user_', '_abc', 'a_b_c_d', '___', '_']) id);

-- Validity: optional multi-segment prefix, then a non-empty base62 body.
SELECT id, isValidPrefixedID(id) FROM (SELECT arrayJoin([
    'user_NffrFeUfNV2Hib',
    'ch_test_51TpZvW',
    'abc123',
    '123abc',
    'user_9x',
    'a_1_b',
    '',
    'user_',
    '_abc',
    'user__x',
    '1user_x',
    'user-x',
    'user x',
    'user_x!']) id);

-- Validity with an expected prefix.
SELECT isValidPrefixedID('user_NffrFeUfNV2Hib', 'user');
SELECT isValidPrefixedID('user_NffrFeUfNV2Hib', 'ch');
SELECT isValidPrefixedID('ch_test_51TpZvW', 'ch_test');
SELECT isValidPrefixedID('ch_test_51TpZvW', 'ch');
SELECT isValidPrefixedID('abc123', 'abc');
SELECT isValidPrefixedID('user__x', 'user_');
SELECT isValidPrefixedID('user_NffrFeUfNV2Hib', ''); -- { serverError BAD_ARGUMENTS }

-- Generator: format, default and usertom body length, multi-segment prefix, non-constant arguments.
SELECT match(generatePrefixedID('user'), '^user_[0-9A-Za-z]{22}$');
SELECT match(generatePrefixedID('ch_test', 10), '^ch_test_[0-9A-Za-z]{10}$');
SELECT match(generatePrefixedID('a', 1), '^a_[0-9A-Za-z]$');
SELECT match(generatePrefixedID('a', 255), '^a_[0-9A-Za-z]{255}$');
SELECT isValidPrefixedID(generatePrefixedID('user'), 'user');
SELECT prefixedIDPrefix(generatePrefixedID(prefix)) = prefix FROM (SELECT arrayJoin(['user', 'ch_test', 'ord']) prefix);
SELECT length(prefixedIDBody(generatePrefixedID('x', number + 1))) = number + 1 FROM numbers(3);
SELECT prefixedIDBody(generatePrefixedID('x')) != prefixedIDBody(generatePrefixedID('y'));

-- Generator argument validation.
SELECT generatePrefixedID(''); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('_user'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('user_'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('user__test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('1user'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('ch_1test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('ch-test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('user', 0); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('user', 256); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('user', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generatePrefixedID(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prefixedIDPrefix(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isValidPrefixedID('user_x', 'user', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- FixedString inputs
SELECT prefixedIDPrefix(toFixedString('user_NffrFeUfNV2Hib', 19)), prefixedIDBody(toFixedString('user_NffrFeUfNV2Hib', 19)), splitPrefixedID(toFixedString('user_NffrFeUfNV2Hib', 19));
SELECT prefixedIDPrefix(materialize(toFixedString('user_NffrFeUfNV2Hib', 19)));
SELECT isValidPrefixedID(toFixedString('user_NffrFeUfNV2Hib', 19));
SELECT isValidPrefixedID(toFixedString('user_NffrFeUfNV2Hib', 19), 'user');
SELECT isValidPrefixedID(toFixedString('user_x', 8)); -- the zero-byte padding is not base62
SELECT prefixedIDBody(toFixedString('user_x', 8)) == 'x\0\0';
