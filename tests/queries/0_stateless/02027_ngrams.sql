SELECT ngrams('Test', 1);
SELECT ngrams('Test', 2);
SELECT ngrams('Test', 3);
SELECT ngrams('Test', 4);
SELECT ngrams('Test', 5);
SELECT ngrams('😁😈😁😈', 1);
SELECT ngrams('😁😈😁😈', 2);
SELECT ngrams('😁😈😁😈', 3);
SELECT ngrams('😁😈😁😈', 4);
SELECT ngrams('😁😈😁😈', 5);

SELECT ngrams(materialize('Test'), 1);
SELECT ngrams(materialize('Test'), 2);
SELECT ngrams(materialize('Test'), 3);
SELECT ngrams(materialize('Test'), 4);
SELECT ngrams(materialize('Test'), 5);
SELECT ngrams(materialize('😁😈😁😈'), 1);
SELECT ngrams(materialize('😁😈😁😈'), 2);
SELECT ngrams(materialize('😁😈😁😈'), 3);
SELECT ngrams(materialize('😁😈😁😈'), 4);
SELECT ngrams(materialize('😁😈😁😈'), 5);

SELECT ngrams(toFixedString('Test', 4), 1);
SELECT ngrams(toFixedString('Test', 4), 2);
SELECT ngrams(toFixedString('Test', 4), 3);
SELECT ngrams(toFixedString('Test', 4), 4);
SELECT ngrams(toFixedString('Test', 4), 5);
SELECT ngrams(toFixedString('😁😈😁😈', 16), 1);
SELECT ngrams(toFixedString('😁😈😁😈', 16), 2);
SELECT ngrams(toFixedString('😁😈😁😈', 16), 3);
SELECT ngrams(toFixedString('😁😈😁😈', 16), 4);
SELECT ngrams(toFixedString('😁😈😁😈', 16), 5);

SELECT ngrams(materialize(toFixedString('Test', 4)), 1);
SELECT ngrams(materialize(toFixedString('Test', 4)), 2);
SELECT ngrams(materialize(toFixedString('Test', 4)), 3);
SELECT ngrams(materialize(toFixedString('Test', 4)), 4);
SELECT ngrams(materialize(toFixedString('Test', 4)), 5);
SELECT ngrams(materialize(toFixedString('😁😈😁😈', 16)), 1);
SELECT ngrams(materialize(toFixedString('😁😈😁😈', 16)), 2);
SELECT ngrams(materialize(toFixedString('😁😈😁😈', 16)), 3);
SELECT ngrams(materialize(toFixedString('😁😈😁😈', 16)), 4);
SELECT ngrams(materialize(toFixedString('😁😈😁😈', 16)), 5);

SELECT ngrams('Test', 0) -- { serverError BAD_ARGUMENTS }
