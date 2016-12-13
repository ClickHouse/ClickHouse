SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TabSeparated;
SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TSV;

SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TabSeparatedWithNames;
SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TSVWithNames;

SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TabSeparatedWithNamesAndTypes;
SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TSVWithNamesAndTypes;

SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TabSeparatedRaw;
SELECT arrayJoin([1, 2, 3]), 'hello', 'world' FORMAT TSVRaw;
