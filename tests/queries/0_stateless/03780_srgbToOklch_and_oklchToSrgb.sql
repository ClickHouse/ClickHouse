
WITH (127, 66, 37) AS rgb
SELECT
rgb AS original,
(
    round(colorSRGBToOKLCH(rgb, 2.2).1, 6),
    round(colorSRGBToOKLCH(rgb, 2.2).2, 6),
    round(colorSRGBToOKLCH(rgb, 2.2).3, 6)
) AS oklch,
(
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(rgb, 2.2), 2.2).1, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(rgb, 2.2), 2.2).2, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(rgb, 2.2), 2.2).3, 3)
) AS roundtrip;

SELECT
gamma,
original,
(
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, gamma), gamma).1, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, gamma), gamma).2, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, gamma), gamma).3, 3)
) AS roundtrip
FROM (
    SELECT arrayJoin([1.0, 1.8, 2.2, 2.4, 2.8]) AS gamma, (127, 66, 37) AS original
);

SELECT
color_name,
original,
(
    round(colorSRGBToOKLCH(original, 2.2).1, 6),
    round(colorSRGBToOKLCH(original, 2.2).2, 6),
    round(colorSRGBToOKLCH(original, 2.2).3, 6)
) AS oklch,
(
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, 2.2), 2.2).1, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, 2.2), 2.2).2, 3),
    round(colorOKLCHToSRGB(colorSRGBToOKLCH(original, 2.2), 2.2).3, 3)
) AS roundtrip
FROM (
        SELECT
            arrayJoin(['Black', 'White', 'Red', 'Green', 'Blue', 'Yellow', 'Cyan', 'Magenta', 'Gray', 'Dark Gray', 'Light Gray']) AS color_name,
            arrayJoin([(0, 0, 0), (255, 255, 255), (255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0), (0, 255, 255), (255, 0, 255), (128, 128, 128), (64, 64, 64), (192, 192, 192)]) AS original
);

SELECT
oklch_input,
(
    round(colorOKLCHToSRGB(oklch_input, 2.2).1, 3),
    round(colorOKLCHToSRGB(oklch_input, 2.2).2, 3),
    round(colorOKLCHToSRGB(oklch_input, 2.2).3, 3)
) AS rgb_output
FROM (
        SELECT arrayJoin(
                            [
                                (0.0, 0.0, 0.0),
                                (1.0, 0.0, 0.0),
                                (0.5, 0.0, 0.0),
                                (0.628, 0.258, 29.2)
                            ]
        ) AS oklch_input
);

SELECT
hue,
(
    round(colorOKLCHToSRGB((0.7, 0.15, hue), 2.2).1, 3),
    round(colorOKLCHToSRGB((0.7, 0.15, hue), 2.2).2, 3),
    round(colorOKLCHToSRGB((0.7, 0.15, hue), 2.2).3, 3)
) AS rgb_output
FROM ( SELECT arrayJoin([0, 30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330]) AS hue );