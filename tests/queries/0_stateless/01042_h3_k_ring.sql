-- Tags: no-unbundled, no-fasttest

SELECT arraySort(h3kRing(581276613233082367, 1));
SELECT h3kRing(581276613233082367, 0);
SELECT h3kRing(581276613233082367, -1); -- { serverError 12 }
