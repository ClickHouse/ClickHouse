WITH 'abb' AS b, 'abc' AS c, 'abd' AS d, toFixedString(b, 5) AS bf, toFixedString(c, 5) AS cf, toFixedString(d, 5) AS df
SELECT 
    b = b, b > b, b < b,
    b = c, b > c, b < c,
    b = d, b > d, b < d,
    b = bf, b > bf, b < bf,
    b = cf, b > cf, b < cf,
    b = df, b > df, b < df,

    c = b, c > b, c < b,
    c = c, c > c, c < c,
    c = d, c > d, c < d,
    c = bf, c > bf, c < bf,
    c = cf, c > cf, c < cf,
    c = df, c > df, c < df,

    d = b, d > b, d < b,
    d = c, d > c, d < c,
    d = d, d > d, d < d,
    d = bf, d > bf, d < bf,
    d = cf, d > cf, d < cf,
    d = df, d > df, d < df,

    bf = b, bf > b, bf < b,
    bf = c, bf > c, bf < c,
    bf = d, bf > d, bf < d,
    bf = bf, bf > bf, bf < bf,
    bf = cf, bf > cf, bf < cf,
    bf = df, bf > df, bf < df,

    cf = b, cf > b, cf < b,
    cf = c, cf > c, cf < c,
    cf = d, cf > d, cf < d,
    cf = bf, cf > bf, cf < bf,
    cf = cf, cf > cf, cf < cf,
    cf = df, cf > df, cf < df,

    df = b, df > b, df < b,
    df = c, df > c, df < c,
    df = d, df > d, df < d,
    df = bf, df > bf, df < bf,
    df = cf, df > cf, df < cf,
    df = df, df > df, df < df

FORMAT Vertical;
