-- Tags: no-fasttest

-- Setting disabled by default -> SUPPORT_IS_DISABLED
SELECT ipcryptEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff')); -- { serverError SUPPORT_IS_DISABLED }
SELECT ipcryptDecrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff')); -- { serverError SUPPORT_IS_DISABLED }
SELECT ipcryptPfxEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')); -- { serverError SUPPORT_IS_DISABLED }
SELECT ipcryptPfxDecrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_ipcrypt_functions = 1;

-- Bad key lengths
SELECT ipcryptEncrypt(toIPv4('1.2.3.4'), 'tooshort'); -- { serverError BAD_ARGUMENTS }
SELECT ipcryptEncrypt(toIPv4('1.2.3.4'), 'toolong_toolong_toolong_toolong_toolong'); -- { serverError BAD_ARGUMENTS }
SELECT ipcryptPfxEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff')); -- { serverError BAD_ARGUMENTS }

-- Malformed hex keys (correct length but invalid hex characters)
SELECT ipcryptEncrypt(toIPv4('1.2.3.4'), 'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ'); -- { serverError BAD_ARGUMENTS }

-- Malformed string IP input
SELECT ipcryptEncrypt('not_an_ip', unhex('00112233445566778899aabbccddeeff')); -- { serverError BAD_ARGUMENTS }
SELECT ipcryptEncrypt('999.999.999.999', unhex('00112233445566778899aabbccddeeff')); -- { serverError BAD_ARGUMENTS }

-- PFX key with identical halves
SELECT ipcryptPfxEncrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff')); -- { serverError BAD_ARGUMENTS }
SELECT ipcryptPfxDecrypt(toIPv4('1.2.3.4'), unhex('00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff')); -- { serverError BAD_ARGUMENTS }

-- Wrong argument types
SELECT ipcryptEncrypt(42, unhex('00112233445566778899aabbccddeeff')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ipcryptEncrypt(toIPv4('1.2.3.4'), 123); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
