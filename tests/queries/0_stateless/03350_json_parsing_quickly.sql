-- Tags: no-fasttest
-- ^ because of base64, which is only present in full builds
SELECT * FROM format(RowBinary, 'x JSON', substring(base64Decode(
'alNPTgr/DUMnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycwJ////////wNuJycnJycnBQAnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycnJycnAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACcnJycnJycnJyck/0gFAA=='
), 6)) SETTINGS enable_json_type = 1, type_json_skip_duplicated_paths = 1; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
