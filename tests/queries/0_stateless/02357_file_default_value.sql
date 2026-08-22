SELECT file('nonexistent.txt'); -- { serverError FILE_DOESNT_EXIST }
SELECT file('nonexistent.txt', 'default');
SELECT file('nonexistent.txt', NULL);
