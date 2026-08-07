SELECT arrayJoin([3, 1, 2]) SETTINGS extremes = 1;
SELECT arrayJoin([nan, 1, 2]) SETTINGS extremes = 1;
SELECT arrayJoin([3, nan, 2]) SETTINGS extremes = 1;
SELECT arrayJoin([3, 1, nan]) SETTINGS extremes = 1;
SELECT arrayJoin([nan, nan, 2]) SETTINGS extremes = 1;
SELECT arrayJoin([nan, 1, nan]) SETTINGS extremes = 1;
SELECT arrayJoin([3, nan, nan]) SETTINGS extremes = 1;
SELECT arrayJoin([nan, nan, nan]) SETTINGS extremes = 1;
