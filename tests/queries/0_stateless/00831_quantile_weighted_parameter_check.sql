SELECT quantileExactWeighted(0.5)(number, number) FROM numbers(10);
SELECT quantileExactWeighted(0.5)(number, 0.1) FROM numbers(10); -- { serverError 43 }
