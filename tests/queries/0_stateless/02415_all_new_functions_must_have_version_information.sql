-- This outputs the list of functions without version information.
-- No new items in the list should appear. Please help shorten this list down to zero elements.
SELECT name FROM system.functions WHERE NOT is_aggregate AND origin = 'System' AND alias_to = '' AND introduced_in == ''
AND name NOT IN (
     -- these functions are not enabled in fast test
    'aes_decrypt_mysql', 'aes_encrypt_mysql', 'decrypt', 'encrypt',
    'convertCharset',
    'detectLanguage', 'detectLanguageMixed',
    'geoToH3',
    'h3CellAreaM2', 'h3CellAreaRads2', 'h3Distance', 'h3EdgeAngle', 'h3EdgeLengthKm', 'h3EdgeLengthM', 'h3ExactEdgeLengthKm', 'h3ExactEdgeLengthM', 'h3ExactEdgeLengthRads', 'h3GetBaseCell',
    'h3GetDestinationIndexFromUnidirectionalEdge', 'h3GetFaces', 'h3GetIndexesFromUnidirectionalEdge', 'h3GetOriginIndexFromUnidirectionalEdge', 'h3GetPentagonIndexes', 'h3GetRes0Indexes',
    'h3GetResolution', 'h3GetUnidirectionalEdge', 'h3GetUnidirectionalEdgeBoundary', 'h3GetUnidirectionalEdgesFromHexagon', 'h3HexAreaKm2', 'h3HexAreaM2', 'h3HexRing', 'h3IndexesAreNeighbors',
    'h3IsPentagon', 'h3IsResClassIII', 'h3IsValid', 'h3Line', 'h3NumHexagons', 'h3PointDistKm', 'h3PointDistM', 'h3PointDistRads', 'h3ToCenterChild', 'h3ToChildren', 'h3ToGeo',
    'h3ToGeoBoundary', 'h3ToParent', 'h3ToString', 'h3UnidirectionalEdgeIsValid', 'h3kRing', 'stringToH3',
    'geoToS2', 's2CapContains', 's2CapUnion', 's2CellsIntersect', 's2GetNeighbors', 's2RectAdd', 's2RectContains', 's2RectIntersection', 's2RectUnion', 's2ToGeo',
    'normalizeUTF8NFC', 'normalizeUTF8NFD', 'normalizeUTF8NFKC', 'normalizeUTF8NFKD',
    'lemmatize', 'tokenize', 'stem', 'synonyms', 'kql_array_sort_asc', 'kql_array_sort_desc',
    'detectCharset', 'detectLanguageUnknown', 'detectProgrammingLanguage', 'detectTonality', 'bech32Encode', 'bech32Decode',
    'BLAKE3', 'JSONMergePatch', 'MD4', 'MD5', 'RIPEMD160', 'SHA1', 'SHA224', 'SHA256', 'SHA384', 'SHA512', 'SHA512_256', 'ULIDStringToDateTime', 'generateULID', 'halfMD5',
    'idnaDecode', 'idnaEncode', 'keccak256', 'punycodeDecode', 'punycodeEncode', 'seriesPeriodDetectFFT', 'sqidDecode', 'sqidEncode', 'tryDecrypt', 'tryIdnaEncode', 'tryPunycodeDecode', 'uniqThetaIntersect',
    'uniqThetaNot', 'uniqThetaUnion'
) ORDER BY name;
