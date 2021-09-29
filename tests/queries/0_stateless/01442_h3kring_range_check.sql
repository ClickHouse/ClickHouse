-- Tags: no-unbundled, no-fasttest

SELECT h3kRing(581276613233082367, 65535); -- { serverError 12 }
SELECT h3kRing(581276613233082367, -1); -- { serverError 12 }
SELECT length(h3kRing(111111111111, 1000));
SELECT h3kRing(581276613233082367, nan); -- { serverError 43 }
