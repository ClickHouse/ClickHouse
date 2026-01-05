
WITH (127,66,37) AS rgb
SELECT
    rgb AS original,
    colorSRGBToOKLAB(rgb, 2.2) AS oklab,
    colorOKLABToSRGB(colorSRGBToOKLAB(rgb, 2.2), 2.2) AS roundtrip;