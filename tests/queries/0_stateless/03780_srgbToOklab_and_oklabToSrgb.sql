
WITH (127, 66, 37) AS rgb
SELECT rgb AS
original,
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


SELECT gamma,
original,
(
    round(colorOKLABToSRGB(colorSRGBToOKLAB(original, gamma), gamma).1, 3),
    round(colorOKLABToSRGB(colorSRGBToOKLAB(original, gamma), gamma).2, 3),
    round(colorOKLABToSRGB(colorSRGBToOKLAB(original, gamma), gamma).3, 3)
) AS roundtrip
FROM (
    SELECT arrayJoin([1.0, 1.8, 2.2, 2.4, 2.8]) AS gamma, (127, 66, 37) AS original
);

SELECT
color_name,
original,
(
    round(colorSRGBToOKLAB(original, 2.2).1, 6),
    round(colorSRGBToOKLAB(original, 2.2).2, 6),
    round(colorSRGBToOKLAB(original, 2.2).3, 6)
) AS oklab,
(
     round(colorOKLABToSRGB(colorSRGBToOKLAB(original, 2.2), 2.2).1, 3),
     round(colorOKLABToSRGB(colorSRGBToOKLAB(original, 2.2), 2.2).2, 3),
     round(colorOKLABToSRGB(colorSRGBToOKLAB(original, 2.2), 2.2).3, 3)
) AS roundtrip
FROM (
    SELECT
        arrayJoin(['Black', 'White', 'Red', 'Green', 'Blue', 'Yellow', 'Cyan', 'Magenta', 'Gray', 'Dark Gray', 'Light Gray']) AS color_name,
        arrayJoin([(0, 0, 0), (255, 255, 255), (255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0), (0, 255, 255), (255, 0, 255), (128, 128, 128), (64, 64, 64), (192, 192, 192)]) AS original
);

SELECT
oklab_input,
(
     round(colorOKLABToSRGB(oklab_input, 2.2).1, 3),
     round(colorOKLABToSRGB(oklab_input, 2.2).2, 3),
     round(colorOKLABToSRGB(oklab_input, 2.2).3, 3)
) AS rgb_output
FROM (
     SELECT arrayJoin([
                        (0.0, 0.0, 0.0),      -- Should produce black
                        (1.0, 0.0, 0.0),      -- Should produce white
                        (0.5, 0.0, 0.0),      -- Mid gray
                        (0.628, 0.225, 0.126) -- Approximate red
                      ]

     ) AS oklab_input
);