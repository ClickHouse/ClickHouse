-- Valid TypeIDs from the official spec test data (spec/valid.yml)
SELECT typeIDToUUID(t) FROM (SELECT arrayJoin([
    '00000000000000000000000000',
    '00000000000000000000000001',
    '0000000000000000000000000a',
    '0000000000000000000000000g',
    '00000000000000000000000010',
    '7zzzzzzzzzzzzzzzzzzzzzzzzz',
    'prefix_0123456789abcdefghjkmnpqrs',
    'prefix_01h455vb4pex5vsknk084sn02q',
    'pre_fix_00000000000000000000000000']) t);

SELECT typeIDPrefix(t) FROM (SELECT arrayJoin([
    '00000000000000000000000000',
    'prefix_01h455vb4pex5vsknk084sn02q',
    'pre_fix_00000000000000000000000000',
    'a_00000000000000000000000000']) t);

-- Invalid TypeIDs from the official spec test data (spec/invalid.yml)
SELECT tryTypeIDToUUID(t) FROM (SELECT arrayJoin([
    'PREFIX_00000000000000000000000000',
    '12345_00000000000000000000000000',
    'pre.fix_00000000000000000000000000',
    '  prefix_00000000000000000000000000',
    'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl_00000000000000000000000000',
    '_00000000000000000000000000',
    '_',
    'prefix_1234567890123456789012345',
    'prefix_123456789012345678901234567',
    'prefix_1234567890123456789012345 ',
    'prefix_0123456789ABCDEFGHJKMNPQRS',
    'prefix_123456789-123456789-123456',
    'prefix_ooooooiiiiiiuuuuuuulllllll',
    'prefix_i23456789ol23456789oi23456',
    'prefix_8zzzzzzzzzzzzzzzzzzzzzzzzz',
    '_prefix_00000000000000000000000000',
    'prefix__00000000000000000000000000',
    '',
    'prefix_']) t);

SELECT typeIDToUUID('PREFIX_00000000000000000000000000'); -- { serverError INCORRECT_DATA }
SELECT typeIDToUUID(''); -- { serverError INCORRECT_DATA }
SELECT typeIDToUUID('prefix_8zzzzzzzzzzzzzzzzzzzzzzzzz'); -- { serverError INCORRECT_DATA }
SELECT typeIDPrefix('prefix_'); -- { serverError INCORRECT_DATA }
SELECT typeIDToUUID('00000000000000000000000000', 'Second arg'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT typeIDToUUID(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- UUIDToTypeID: with prefix, with empty prefix, and without prefix
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'user');
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'pre_fix');
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), '');
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'));
SELECT UUIDToTypeID(toUUID('00000000-0000-0000-0000-000000000000'), 'nil');
SELECT UUIDToTypeID(toUUID('ffffffff-ffff-ffff-ffff-ffffffffffff'));
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), repeat('a', 63));

-- Invalid prefixes
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'PREFIX'); -- { serverError BAD_ARGUMENTS }
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), '_prefix'); -- { serverError BAD_ARGUMENTS }
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'prefix_'); -- { serverError BAD_ARGUMENTS }
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), 'pre.fix'); -- { serverError BAD_ARGUMENTS }
SELECT UUIDToTypeID(toUUID('01890a5d-ac96-774b-bcce-b302099a8057'), repeat('a', 64)); -- { serverError BAD_ARGUMENTS }
SELECT generateTypeID('PREFIX'); -- { serverError BAD_ARGUMENTS }

-- Roundtrips
SELECT UUIDToTypeID(typeIDToUUID('user_01h455vb4pex5vsknk084sn02q'), typeIDPrefix('user_01h455vb4pex5vsknk084sn02q'));
SELECT typeIDToUUID(UUIDToTypeID(uuid, 'roundtrip')) = uuid FROM (SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') uuid);

-- Non-constant columns
SELECT UUIDToTypeID(typeIDToUUID(t), typeIDPrefix(t)) = t FROM (SELECT arrayJoin([
    'prefix_01h455vb4pex5vsknk084sn02q',
    'pre_fix_00000000000000000000000000',
    '7zzzzzzzzzzzzzzzzzzzzzzzzz',
    'a_0123456789abcdefghjkmnpqrs']) t);

-- generateTypeID: format, prefix extraction, suffix decodability, and UUIDv7 version of the payload
SELECT match(generateTypeID('user'), '^user_[0-7][0123456789abcdefghjkmnpqrstvwxyz]{25}$');
SELECT match(generateTypeID(), '^[0-7][0123456789abcdefghjkmnpqrstvwxyz]{25}$');
SELECT match(generateTypeID(''), '^[0-7][0123456789abcdefghjkmnpqrstvwxyz]{25}$');
SELECT typeIDPrefix(generateTypeID('pre_fix'));
SELECT substring(toString(typeIDToUUID(generateTypeID('user'))), 15, 1);
SELECT typeIDToUUID(generateTypeID('a')) != typeIDToUUID(generateTypeID('b'));

-- FixedString inputs
SELECT typeIDToUUID(toFixedString('01h455vb4pex5vsknk084sn02q', 26));
SELECT typeIDToUUID(materialize(toFixedString('01h455vb4pex5vsknk084sn02q', 26)));
SELECT typeIDToUUID(toFixedString('prefix_01h455vb4pex5vsknk084sn02q', 33));
SELECT typeIDPrefix(toFixedString('prefix_01h455vb4pex5vsknk084sn02q', 33));
SELECT tryTypeIDToUUID(toFixedString('00000000000000000000000000', 27)); -- the zero-byte padding makes it invalid
