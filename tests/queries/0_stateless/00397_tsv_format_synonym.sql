SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparated;
SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSV;

SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedWithNames;
SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVWithNames;

SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedWithNamesAndTypes;
SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVWithNamesAndTypes;

SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TabSeparatedRaw;
SELECT arrayJoin([1, 2, 3]) AS arr, 'hello' AS s1, 'world' AS s2 FORMAT TSVRaw;
