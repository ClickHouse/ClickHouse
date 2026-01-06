
WITH (127, 66, 37) AS rgb
SELECT
rgb AS original,
(
 round(colorSRGBToOKLAB(rgb, 2.2).1, 6),
 round(colorSRGBToOKLAB(rgb, 2.2).2, 6),
 round(colorSRGBToOKLAB(rgb, 2.2).3, 6)
) AS oklab,
(
 round(colorOKLABToSRGB(colorSRGBToOKLAB(rgb, 2.2), 2.2).1, 3),
 round(colorOKLABToSRGB(colorSRGBToOKLAB(rgb, 2.2), 2.2).2, 3),
 round(colorOKLABToSRGB(colorSRGBToOKLAB(rgb, 2.2), 2.2).3, 3)
) AS roundtrip;
