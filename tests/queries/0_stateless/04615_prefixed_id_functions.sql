-- The reference table of split semantics: split at the last underscore, never fails.
SELECT id, prefixedIdPrefix(id), prefixedIdBody(id), splitPrefixedId(id) FROM (SELECT arrayJoin([
    'cus_NffrFeUfNV2Hib',
    'pk_test_51TpZvW',
    'abc123',
    '',
    'cus_',
    '_abc']) id);

-- The scalar functions must agree with the tuple function.
SELECT sum((prefixedIdPrefix(id), prefixedIdBody(id)) != splitPrefixedId(id)) FROM (SELECT arrayJoin([
    'cus_NffrFeUfNV2Hib', 'pk_test_51TpZvW', 'abc123', '', 'cus_', '_abc', 'a_b_c_d', '___', '_']) id);

-- Validity: optional multi-segment prefix, then a non-empty base62 body.
SELECT id, isValidPrefixedId(id) FROM (SELECT arrayJoin([
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
SELECT isValidPrefixedId('cus_NffrFeUfNV2Hib', 'cus');
SELECT isValidPrefixedId('cus_NffrFeUfNV2Hib', 'pk');
SELECT isValidPrefixedId('pk_test_51TpZvW', 'pk_test');
SELECT isValidPrefixedId('pk_test_51TpZvW', 'pk');
SELECT isValidPrefixedId('abc123', 'abc');
SELECT isValidPrefixedId('cus__x', 'cus_');
SELECT isValidPrefixedId('cus_NffrFeUfNV2Hib', ''); -- { serverError BAD_ARGUMENTS }

-- Generator: format, default and custom body length, multi-segment prefix, non-constant arguments.
SELECT match(generatePrefixedId('cus'), '^cus_[0-9A-Za-z]{22}$');
SELECT match(generatePrefixedId('pk_test', 10), '^pk_test_[0-9A-Za-z]{10}$');
SELECT match(generatePrefixedId('a', 1), '^a_[0-9A-Za-z]$');
SELECT match(generatePrefixedId('a', 255), '^a_[0-9A-Za-z]{255}$');
SELECT isValidPrefixedId(generatePrefixedId('cus'), 'cus');
SELECT prefixedIdPrefix(generatePrefixedId(prefix)) = prefix FROM (SELECT arrayJoin(['cus', 'pk_test', 'acct']) prefix);
SELECT length(prefixedIdBody(generatePrefixedId('x', number + 1))) = number + 1 FROM numbers(3);
SELECT prefixedIdBody(generatePrefixedId('x')) != prefixedIdBody(generatePrefixedId('y'));

-- Generator argument validation.
SELECT generatePrefixedId(''); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('_cus'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('cus_'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('cus__test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('1cus'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('pk_1test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('pk-test'); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('cus', 0); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('cus', 256); -- { serverError BAD_ARGUMENTS }
SELECT generatePrefixedId('cus', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT generatePrefixedId(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT prefixedIdPrefix(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT isValidPrefixedId('cus_x', 'cus', 'extra'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
