SELECT file('nonexistent.txt'); -- { serverError 107 }
SELECT file('nonexistent.txt', 'default');
SELECT file('nonexistent.txt', NULL);
