SET enable_analyzer = 1;

SELECT
    arraySum(x -> ((x.1) / ((x.2) * (x.2))), arrayZip(mag, magerr)) / arraySum(x -> (1. / (x * x)), magerr) AS weightedmeanmag,
    arraySum(x -> ((((x.1) - weightedmeanmag) * ((x.1) - weightedmeanmag)) / ((x.2) * (x.2))), arrayZip(mag, magerr)) AS chi2,
    [1, 2, 3, 4] AS mag,
    [0.1, 0.2, 0.1, 0.2] AS magerr;

SELECT
    arraySum(x -> ((x.1) / ((x.2) * (x.2))), arrayZip(mag, magerr)) / arraySum(x -> (1. / (x * x)), magerr) AS weightedmeanmag,
    arraySum(x -> ((((x.1) - weightedmeanmag) * ((x.1) - weightedmeanmag)) / ((x.2) * (x.2))), arrayZip(mag, magerr)) AS chi2,
    [1, 2, 3, 4] AS mag,
    [0.1, 0.2, 0.1, 0.2] AS magerr
WHERE isFinite(chi2)
