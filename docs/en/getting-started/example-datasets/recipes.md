---
toc_priority: 16
toc_title: Recipes Dataset
---

# Recipes Dataset

RecipeNLG dataset is available for download [here](https://recipenlg.cs.put.poznan.pl/dataset). It contains 2.2 million recipes. The size is slightly less than 1 GB.

## Download and Unpack the Dataset

1. Go to the download page [https://recipenlg.cs.put.poznan.pl/dataset](https://recipenlg.cs.put.poznan.pl/dataset).
1. Accept Terms and Conditions and download zip file.
1. Unpack the zip file with `unzip`. You will get the `full_dataset.csv` file.

## Create a Table

Run clickhouse-client and execute the following CREATE query:

``` sql
CREATE TABLE recipes
(
    title String,
    ingredients Array(String),
    directions Array(String),
    link String,
    source LowCardinality(String),
    NER Array(String)
) ENGINE = MergeTree ORDER BY title;
```

## Insert the Data

Run the following command:

``` bash
clickhouse-client --query "
    INSERT INTO recipes
    SELECT
        title,
        JSONExtract(ingredients, 'Array(String)'),
        JSONExtract(directions, 'Array(String)'),
        link,
        source,
        JSONExtract(NER, 'Array(String)')
    FROM input('num UInt32, title String, ingredients String, directions String, link String, source LowCardinality(String), NER String')
    FORMAT CSVWithNames
" --input_format_with_names_use_header 0 --format_csv_allow_single_quote 0 --input_format_allow_errors_num 10 < full_dataset.csv
```

This is a showcase how to parse custom CSV, as it requires multiple tunes.

Explanation:
-   The dataset is in CSV format, but it requires some preprocessing on insertion; we use table function [input](../../sql-reference/table-functions/input.md) to perform preprocessing;
-   The structure of CSV file is specified in the argument of the table function `input`;
-   The field `num` (row number) is unneeded - we parse it from file and ignore;
-   We use `FORMAT CSVWithNames` but the header in CSV will be ignored (by command line parameter `--input_format_with_names_use_header 0`), because the header does not contain the name for the first field;
-   File is using only double quotes to enclose CSV strings; some strings are not enclosed in double quotes, and single quote must not be parsed as the string enclosing - that's why we also add the `--format_csv_allow_single_quote 0` parameter;
-   Some strings from CSV cannot parse, because they contain `\M/` sequence at the beginning of the value; the only value starting with backslash in CSV can be `\N` that is parsed as SQL NULL. We add `--input_format_allow_errors_num 10` parameter and up to ten malformed records can be skipped;
-   There are arrays for ingredients, directions and NER fields; these arrays are represented in unusual form: they are serialized into string as JSON and then placed in CSV - we parse them as String and then use [JSONExtract](../../sql-reference/functions/json-functions/) function to transform it to Array.

## Validate the Inserted Data

By checking the row count:

Query:

``` sql
SELECT count() FROM recipes;
```

Result:

``` text
┌─count()─┐
│ 2231141 │
└─────────┘
```

## Example Queries

### Top Components by the Number of Recipes:

In this example we learn how to use [arrayJoin](../../sql-reference/functions/array-join/) function to expand an array into a set of rows.

Query:

``` sql
SELECT
    arrayJoin(NER) AS k,
    count() AS c
FROM recipes
GROUP BY k
ORDER BY c DESC
LIMIT 50
```

Result:

``` text
┌─k────────────────────┬──────c─┐
│ salt                 │ 890741 │
│ sugar                │ 620027 │
│ butter               │ 493823 │
│ flour                │ 466110 │
│ eggs                 │ 401276 │
│ onion                │ 372469 │
│ garlic               │ 358364 │
│ milk                 │ 346769 │
│ water                │ 326092 │
│ vanilla              │ 270381 │
│ olive oil            │ 197877 │
│ pepper               │ 179305 │
│ brown sugar          │ 174447 │
│ tomatoes             │ 163933 │
│ egg                  │ 160507 │
│ baking powder        │ 148277 │
│ lemon juice          │ 146414 │
│ Salt                 │ 122557 │
│ cinnamon             │ 117927 │
│ sour cream           │ 116682 │
│ cream cheese         │ 114423 │
│ margarine            │ 112742 │
│ celery               │ 112676 │
│ baking soda          │ 110690 │
│ parsley              │ 102151 │
│ chicken              │ 101505 │
│ onions               │  98903 │
│ vegetable oil        │  91395 │
│ oil                  │  85600 │
│ mayonnaise           │  84822 │
│ pecans               │  79741 │
│ nuts                 │  78471 │
│ potatoes             │  75820 │
│ carrots              │  75458 │
│ pineapple            │  74345 │
│ soy sauce            │  70355 │
│ black pepper         │  69064 │
│ thyme                │  68429 │
│ mustard              │  65948 │
│ chicken broth        │  65112 │
│ bacon                │  64956 │
│ honey                │  64626 │
│ oregano              │  64077 │
│ ground beef          │  64068 │
│ unsalted butter      │  63848 │
│ mushrooms            │  61465 │
│ Worcestershire sauce │  59328 │
│ cornstarch           │  58476 │
│ green pepper         │  58388 │
│ Cheddar cheese       │  58354 │
└──────────────────────┴────────┘

50 rows in set. Elapsed: 0.112 sec. Processed 2.23 million rows, 361.57 MB (19.99 million rows/s., 3.24 GB/s.)
```

### The Most Complex Recipes with Strawberry

``` sql
SELECT
    title,
    length(NER),
    length(directions)
FROM recipes
WHERE has(NER, 'strawberry')
ORDER BY length(directions) DESC
LIMIT 10
```

Result:

``` text
┌─title────────────────────────────────────────────────────────────┬─length(NER)─┬─length(directions)─┐
│ Chocolate-Strawberry-Orange Wedding Cake                         │          24 │                126 │
│ Strawberry Cream Cheese Crumble Tart                             │          19 │                 47 │
│ Charlotte-Style Ice Cream                                        │          11 │                 45 │
│ Sinfully Good a Million Layers Chocolate Layer Cake, With Strawb │          31 │                 45 │
│ Sweetened Berries With Elderflower Sherbet                       │          24 │                 44 │
│ Chocolate-Strawberry Mousse Cake                                 │          15 │                 42 │
│ Rhubarb Charlotte with Strawberries and Rum                      │          20 │                 42 │
│ Chef Joey's Strawberry Vanilla Tart                              │           7 │                 37 │
│ Old-Fashioned Ice Cream Sundae Cake                              │          17 │                 37 │
│ Watermelon Cake                                                  │          16 │                 36 │
└──────────────────────────────────────────────────────────────────┴─────────────┴────────────────────┘

10 rows in set. Elapsed: 0.215 sec. Processed 2.23 million rows, 1.48 GB (10.35 million rows/s., 6.86 GB/s.)
```

In this example, we involve [has](../../sql-reference/functions/array-functions/#hasarr-elem) function to filter by array elements and sort by the number of directions.

There is a wedding cake that requires the whole 126 steps to produce! Show that directions:

Query:

``` sql
SELECT arrayJoin(directions)
FROM recipes
WHERE title = 'Chocolate-Strawberry-Orange Wedding Cake'
```

Result:

``` text
┌─arrayJoin(directions)───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Position 1 rack in center and 1 rack in bottom third of oven and preheat to 350F.                                                                                           │
│ Butter one 5-inch-diameter cake pan with 2-inch-high sides, one 8-inch-diameter cake pan with 2-inch-high sides and one 12-inch-diameter cake pan with 2-inch-high sides.   │
│ Dust pans with flour; line bottoms with parchment.                                                                                                                          │
│ Combine 1/3 cup orange juice and 2 ounces unsweetened chocolate in heavy small saucepan.                                                                                    │
│ Stir mixture over medium-low heat until chocolate melts.                                                                                                                    │
│ Remove from heat.                                                                                                                                                           │
│ Gradually mix in 1 2/3 cups orange juice.                                                                                                                                   │
│ Sift 3 cups flour, 2/3 cup cocoa, 2 teaspoons baking soda, 1 teaspoon salt and 1/2 teaspoon baking powder into medium bowl.                                                 │
│ using electric mixer, beat 1 cup (2 sticks) butter and 3 cups sugar in large bowl until blended (mixture will look grainy).                                                 │
│ Add 4 eggs, 1 at a time, beating to blend after each.                                                                                                                       │
│ Beat in 1 tablespoon orange peel and 1 tablespoon vanilla extract.                                                                                                          │
│ Add dry ingredients alternately with orange juice mixture in 3 additions each, beating well after each addition.                                                            │
│ Mix in 1 cup chocolate chips.                                                                                                                                               │
│ Transfer 1 cup plus 2 tablespoons batter to prepared 5-inch pan, 3 cups batter to prepared 8-inch pan and remaining batter (about 6 cups) to 12-inch pan.                   │
│ Place 5-inch and 8-inch pans on center rack of oven.                                                                                                                        │
│ Place 12-inch pan on lower rack of oven.                                                                                                                                    │
│ Bake cakes until tester inserted into center comes out clean, about 35 minutes.                                                                                             │
│ Transfer cakes in pans to racks and cool completely.                                                                                                                        │
│ Mark 4-inch diameter circle on one 6-inch-diameter cardboard cake round.                                                                                                    │
│ Cut out marked circle.                                                                                                                                                      │
│ Mark 7-inch-diameter circle on one 8-inch-diameter cardboard cake round.                                                                                                    │
│ Cut out marked circle.                                                                                                                                                      │
│ Mark 11-inch-diameter circle on one 12-inch-diameter cardboard cake round.                                                                                                  │
│ Cut out marked circle.                                                                                                                                                      │
│ Cut around sides of 5-inch-cake to loosen.                                                                                                                                  │
│ Place 4-inch cardboard over pan.                                                                                                                                            │
│ Hold cardboard and pan together; turn cake out onto cardboard.                                                                                                              │
│ Peel off parchment.Wrap cakes on its cardboard in foil.                                                                                                                     │
│ Repeat turning out, peeling off parchment and wrapping cakes in foil, using 7-inch cardboard for 8-inch cake and 11-inch cardboard for 12-inch cake.                        │
│ Using remaining ingredients, make 1 more batch of cake batter and bake 3 more cake layers as described above.                                                               │
│ Cool cakes in pans.                                                                                                                                                         │
│ Cover cakes in pans tightly with foil.                                                                                                                                      │
│ (Can be prepared ahead.                                                                                                                                                     │
│ Let stand at room temperature up to 1 day or double-wrap all cake layers and freeze up to 1 week.                                                                           │
│ Bring cake layers to room temperature before using.)                                                                                                                        │
│ Place first 12-inch cake on its cardboard on work surface.                                                                                                                  │
│ Spread 2 3/4 cups ganache over top of cake and all the way to edge.                                                                                                         │
│ Spread 2/3 cup jam over ganache, leaving 1/2-inch chocolate border at edge.                                                                                                 │
│ Drop 1 3/4 cups white chocolate frosting by spoonfuls over jam.                                                                                                             │
│ Gently spread frosting over jam, leaving 1/2-inch chocolate border at edge.                                                                                                 │
│ Rub some cocoa powder over second 12-inch cardboard.                                                                                                                        │
│ Cut around sides of second 12-inch cake to loosen.                                                                                                                          │
│ Place cardboard, cocoa side down, over pan.                                                                                                                                 │
│ Turn cake out onto cardboard.                                                                                                                                               │
│ Peel off parchment.                                                                                                                                                         │
│ Carefully slide cake off cardboard and onto filling on first 12-inch cake.                                                                                                  │
│ Refrigerate.                                                                                                                                                                │
│ Place first 8-inch cake on its cardboard on work surface.                                                                                                                   │
│ Spread 1 cup ganache over top all the way to edge.                                                                                                                          │
│ Spread 1/4 cup jam over, leaving 1/2-inch chocolate border at edge.                                                                                                         │
│ Drop 1 cup white chocolate frosting by spoonfuls over jam.                                                                                                                  │
│ Gently spread frosting over jam, leaving 1/2-inch chocolate border at edge.                                                                                                 │
│ Rub some cocoa over second 8-inch cardboard.                                                                                                                                │
│ Cut around sides of second 8-inch cake to loosen.                                                                                                                           │
│ Place cardboard, cocoa side down, over pan.                                                                                                                                 │
│ Turn cake out onto cardboard.                                                                                                                                               │
│ Peel off parchment.                                                                                                                                                         │
│ Slide cake off cardboard and onto filling on first 8-inch cake.                                                                                                             │
│ Refrigerate.                                                                                                                                                                │
│ Place first 5-inch cake on its cardboard on work surface.                                                                                                                   │
│ Spread 1/2 cup ganache over top of cake and all the way to edge.                                                                                                            │
│ Spread 2 tablespoons jam over, leaving 1/2-inch chocolate border at edge.                                                                                                   │
│ Drop 1/3 cup white chocolate frosting by spoonfuls over jam.                                                                                                                │
│ Gently spread frosting over jam, leaving 1/2-inch chocolate border at edge.                                                                                                 │
│ Rub cocoa over second 6-inch cardboard.                                                                                                                                     │
│ Cut around sides of second 5-inch cake to loosen.                                                                                                                           │
│ Place cardboard, cocoa side down, over pan.                                                                                                                                 │
│ Turn cake out onto cardboard.                                                                                                                                               │
│ Peel off parchment.                                                                                                                                                         │
│ Slide cake off cardboard and onto filling on first 5-inch cake.                                                                                                             │
│ Chill all cakes 1 hour to set filling.                                                                                                                                      │
│ Place 12-inch tiered cake on its cardboard on revolving cake stand.                                                                                                         │
│ Spread 2 2/3 cups frosting over top and sides of cake as a first coat.                                                                                                      │
│ Refrigerate cake.                                                                                                                                                           │
│ Place 8-inch tiered cake on its cardboard on cake stand.                                                                                                                    │
│ Spread 1 1/4 cups frosting over top and sides of cake as a first coat.                                                                                                      │
│ Refrigerate cake.                                                                                                                                                           │
│ Place 5-inch tiered cake on its cardboard on cake stand.                                                                                                                    │
│ Spread 3/4 cup frosting over top and sides of cake as a first coat.                                                                                                         │
│ Refrigerate all cakes until first coats of frosting set, about 1 hour.                                                                                                      │
│ (Cakes can be made to this point up to 1 day ahead; cover and keep refrigerate.)                                                                                            │
│ Prepare second batch of frosting, using remaining frosting ingredients and following directions for first batch.                                                            │
│ Spoon 2 cups frosting into pastry bag fitted with small star tip.                                                                                                           │
│ Place 12-inch cake on its cardboard on large flat platter.                                                                                                                  │
│ Place platter on cake stand.                                                                                                                                                │
│ Using icing spatula, spread 2 1/2 cups frosting over top and sides of cake; smooth top.                                                                                     │
│ Using filled pastry bag, pipe decorative border around top edge of cake.                                                                                                    │
│ Refrigerate cake on platter.                                                                                                                                                │
│ Place 8-inch cake on its cardboard on cake stand.                                                                                                                           │
│ Using icing spatula, spread 1 1/2 cups frosting over top and sides of cake; smooth top.                                                                                     │
│ Using pastry bag, pipe decorative border around top edge of cake.                                                                                                           │
│ Refrigerate cake on its cardboard.                                                                                                                                          │
│ Place 5-inch cake on its cardboard on cake stand.                                                                                                                           │
│ Using icing spatula, spread 3/4 cup frosting over top and sides of cake; smooth top.                                                                                        │
│ Using pastry bag, pipe decorative border around top edge of cake, spooning more frosting into bag if necessary.                                                             │
│ Refrigerate cake on its cardboard.                                                                                                                                          │
│ Keep all cakes refrigerated until frosting sets, about 2 hours.                                                                                                             │
│ (Can be prepared 2 days ahead.                                                                                                                                              │
│ Cover loosely; keep refrigerated.)                                                                                                                                          │
│ Place 12-inch cake on platter on work surface.                                                                                                                              │
│ Press 1 wooden dowel straight down into and completely through center of cake.                                                                                              │
│ Mark dowel 1/4 inch above top of frosting.                                                                                                                                  │
│ Remove dowel and cut with serrated knife at marked point.                                                                                                                   │
│ Cut 4 more dowels to same length.                                                                                                                                           │
│ Press 1 cut dowel back into center of cake.                                                                                                                                 │
│ Press remaining 4 cut dowels into cake, positioning 3 1/2 inches inward from cake edges and spacing evenly.                                                                 │
│ Place 8-inch cake on its cardboard on work surface.                                                                                                                         │
│ Press 1 dowel straight down into and completely through center of cake.                                                                                                     │
│ Mark dowel 1/4 inch above top of frosting.                                                                                                                                  │
│ Remove dowel and cut with serrated knife at marked point.                                                                                                                   │
│ Cut 3 more dowels to same length.                                                                                                                                           │
│ Press 1 cut dowel back into center of cake.                                                                                                                                 │
│ Press remaining 3 cut dowels into cake, positioning 2 1/2 inches inward from edges and spacing evenly.                                                                      │
│ Using large metal spatula as aid, place 8-inch cake on its cardboard atop dowels in 12-inch cake, centering carefully.                                                      │
│ Gently place 5-inch cake on its cardboard atop dowels in 8-inch cake, centering carefully.                                                                                  │
│ Using citrus stripper, cut long strips of orange peel from oranges.                                                                                                         │
│ Cut strips into long segments.                                                                                                                                              │
│ To make orange peel coils, wrap peel segment around handle of wooden spoon; gently slide peel off handle so that peel keeps coiled shape.                                   │
│ Garnish cake with orange peel coils, ivy or mint sprigs, and some berries.                                                                                                  │
│ (Assembled cake can be made up to 8 hours ahead.                                                                                                                            │
│ Let stand at cool room temperature.)                                                                                                                                        │
│ Remove top and middle cake tiers.                                                                                                                                           │
│ Remove dowels from cakes.                                                                                                                                                   │
│ Cut top and middle cakes into slices.                                                                                                                                       │
│ To cut 12-inch cake: Starting 3 inches inward from edge and inserting knife straight down, cut through from top to bottom to make 6-inch-diameter circle in center of cake. │
│ Cut outer portion of cake into slices; cut inner portion into slices and serve with strawberries.                                                                           │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

126 rows in set. Elapsed: 0.011 sec. Processed 8.19 thousand rows, 5.34 MB (737.75 thousand rows/s., 480.59 MB/s.)
```

### Online Playground

The dataset is also available in the [Online Playground](https://gh-api.clickhouse.com/play?user=play#U0VMRUNUCiAgICBhcnJheUpvaW4oTkVSKSBBUyBrLAogICAgY291bnQoKSBBUyBjCkZST00gcmVjaXBlcwpHUk9VUCBCWSBrCk9SREVSIEJZIGMgREVTQwpMSU1JVCA1MA==).

[Original article](https://clickhouse.com/docs/en/getting-started/example-datasets/recipes/) <!--hide-->
