-- The reference table of split semantics: split at the last underscore, never fails.
SELECT id, prefixedIDPrefix(id), prefixedIDBody(id), splitPrefixedID(id) FROM (SELECT arrayJoin([
    'cus_NffrFeUfNV2Hib',
    'pk_test_51TpZvW',
    'abc123',
    '',
    'cus_',
    '_abc']) id);

-- The scalar functions must agree with the tuple function.
SELECT sum((prefixedIDPrefix(id), prefixedIDBody(id)) != splitPrefixedID(id)) FROM (SELECT arrayJoin([
    'cus_NffrFeUfNV2Hib', 'pk_test_51TpZvW', 'abc123', '', 'cus_', '_abc', 'a_b_c_d', '___', '_']) id);

-- Validity: optional multi-segment prefix, then a non-empty base62 body.
SELECT id, isValidPrefixedID(id) FROM (SELECT arrayJoin([
    'cus_NffrFeUfNV2Hib',
    'pk_test_51TpZvW',
    'abc123',
    '123abc',
    'cus_9x',
    'a_1_b',
    '',
    'cus_',
    '_abc',
    'cus__x',
    '1cus_x',
    'cus-x',
    'cus x',
    'cus_x!']) id);

-- Validity with an expected prefix.
SELECT isValidPrefixedID('cus_NffrFeUfNV2Hib', 'cus');
SELECT isValidPrefixedID('cus_NffrFeUfNV2Hib', 'pk');
SELECT isValidPrefixedID('pk_test_51TpZvW', 'pk_test');
SELECT isValidPrefixedID('pk_test_51TpZvW', 'pk');
SELECT isValidPrefixedID('abc123', 'abc');
SELECT isValidPrefixedID('cus__x', 'cus_');
SELECT isValidPrefixedID('cus_NffrFeUfNV2Hib', ''); -- { serverError BAD_ARGUMENTS }

-- Generator: format, default and custom body length, multi-segment prefix, non-constant arguments.
SELECT match(generatePrefixedID('cus'), '^cus_[0-9A-Za-z]{22}$');
SELECT match(generatePrefixedID('pk_test', 10), '^pk_test_[0-9A-Za-z]{10}$');
SELECT match(generatePrefixedID('a', 1), '^a_[0-9A-Za-z]$');
SELECT match(generatePrefixedID('a', 255), '^a_[0-9A-Za-z]{255}$');
SELECT isValidPrefixedID(generatePrefixedID('cus'), 'cus');
SELECT prefixedIDPrefix(generatePrefixedID(prefix)) = prefix FROM (SELECT arrayJoin(['cus', 'pk_test', 'acct']) prefix);
SELECT length(prefixedIDBody(generatePrefixedID('x', number + 1))) = number + 1 FROM numbers(3);
SELECT prefixedIDBody(generatePrefixedID('x')) != prefixedIDBody(generatePrefixedID('y'));

-- Generator argument validation.
SELECT generatePrefixedID(''); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('_cus'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('cus_'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('cus__test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('1cus'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('pk_1test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('pk-test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('cus', 0); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('cus', 256); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedID('cus', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generatePrefixedID(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prefixedIDPrefix(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isValidPrefixedID('cus_x', 'cus', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
