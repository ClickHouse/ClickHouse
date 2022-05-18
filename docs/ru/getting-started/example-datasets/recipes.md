---
sidebar_position: 16
sidebar_label: Набор данных кулинарных рецептов
---

# Набор данных кулинарных рецептов

Набор данных кулинарных рецептов от RecipeNLG доступен для загрузки [здесь](https://recipenlg.cs.put.poznan.pl/dataset). Он содержит 2.2 миллиона рецептов, а его размер чуть меньше 1 ГБ.

## Загрузите и распакуйте набор данных

1. Перейдите на страницу загрузки [https://recipenlg.cs.put.poznan.pl/dataset](https://recipenlg.cs.put.poznan.pl/dataset).
1. Примите Правила и условия и скачайте zip-архив с набором данных.
1. Распакуйте zip-архив и вы получите файл `full_dataset.csv`.

## Создайте таблицу

Запустите клиент ClickHouse и выполните следующий запрос для создания таблицы `recipes`:

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

## Добавьте данные в таблицу

Чтобы добавить данные из файла `full_dataset.csv` в таблицу `recipes`, выполните команду:

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

Это один из примеров анализа пользовательских CSV-файлов с применением специальных настроек.

Пояснение:
-   набор данных представлен в формате CSV и требует некоторой предварительной обработки при вставке. Для предварительной обработки используется табличная функция [input](../../sql-reference/table-functions/input.md);
-   структура CSV-файла задается в аргументе табличной функции `input`;
-   поле `num` (номер строки) не нужно — оно считывается из файла, но игнорируется;
-   при загрузке используется `FORMAT CSVWithNames`, но заголовок в CSV будет проигнорирован (параметром командной строки `--input_format_with_names_use_header 0`), поскольку заголовок не содержит имени первого поля;
-   в файле CSV для разделения строк используются только двойные кавычки. Но некоторые строки не заключены в двойные кавычки, и чтобы одинарная кавычка не рассматривалась как заключающая, используется параметр `--format_csv_allow_single_quote 0`;
-   некоторые строки из CSV не могут быть считаны корректно, поскольку они начинаются с символов`\M/`, тогда как в CSV начинаться с обратной косой черты могут только символы `\N`, которые распознаются как `NULL` в SQL. Поэтому используется параметр `--input_format_allow_errors_num 10`, разрешающий пропустить до десяти некорректных записей;
-   массивы `ingredients`, `directions` и `NER` представлены в необычном виде: они сериализуются в строку формата JSON, а затем помещаются в CSV — тогда они могут считываться и обрабатываться как обычные строки (`String`). Чтобы преобразовать строку в массив, используется функция [JSONExtract](../../sql-reference/functions/json-functions.md).

## Проверьте добавленные данные

Чтобы проверить добавленные данные, подсчитайте количество строк в таблице:

Запрос:

``` sql
SELECT count() FROM recipes;
```

Результат:

``` text
┌─count()─┐
│ 2231141 │
└─────────┘
```

## Примеры запросов

### Самые упоминаемые ингридиенты в рецептах:

В этом примере вы узнаете, как развернуть массив в набор строк с помощью функции  [arrayJoin](../../sql-reference/functions/array-join.md).

Запрос:

``` sql
SELECT
    arrayJoin(NER) AS k,
    count() AS c
FROM recipes
GROUP BY k
ORDER BY c DESC
LIMIT 50
```

Результат:

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

### Самые сложные рецепты с клубникой

Запрос:

``` sql
SELECT
    title,
    length(NER),
    length(directions)
FROM recipes
WHERE has(NER, 'strawberry')
ORDER BY length(directions) DESC
LIMIT 10;
```

Результат:

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

В этом примере используется функция [has](../../sql-reference/functions/array-functions.md#hasarr-elem) для проверки вхождения элемента в массив, а также сортировка по количеству шагов (`length(directions)`).

Существует свадебный торт, который требует целых 126 шагов для производства! Рассмотрим эти шаги:

Запрос:

``` sql
SELECT arrayJoin(directions)
FROM recipes
WHERE title = 'Chocolate-Strawberry-Orange Wedding Cake';
```

Результат:

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

Этот набор данных доступен в [Online Playground](https://gh-api.clickhouse.com/play?user=play#U0VMRUNUCiAgICBhcnJheUpvaW4oTkVSKSBBUyBrLAogICAgY291bnQoKSBBUyBjCkZST00gcmVjaXBlcwpHUk9VUCBCWSBrCk9SREVSIEJZIGMgREVTQwpMSU1JVCA1MA==).

[Оригинальная статья](https://clickhouse.com/docs/ru/getting-started/example-datasets/recipes/) <!--hide-->
