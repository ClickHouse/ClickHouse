SELECT startsWith(s, 'He') FROM (SELECT arrayJoin(['', 'H', 'He', 'Hellow', '3434', 'fffffffffdHe']) AS s);
SELECT startsWith(s, '') FROM (SELECT arrayJoin(['', 'h', 'hi']) AS s);
