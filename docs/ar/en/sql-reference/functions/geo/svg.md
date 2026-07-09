---
description: 'توثيق Svg'
sidebar_label: 'SVG'
slug: /sql-reference/functions/geo/svg
title: 'دوال لإنشاء صور SVG من بيانات Geo'
doc_type: 'مرجع'
---

<div id="svg">
  ## Svg
</div>

يعيد سلسلة نصية تتضمن وسوم عناصر SVG محددة من البيانات الجغرافية.

**الصياغة**

```sql
Svg(geometry,[style])
```

الأسماء البديلة: `SVG`, `svg`

**المعلمات**

* `geometry` — بيانات Geo. [Geo](../../data-types/geo).
* `style` — اسم نمط اختياري. [String](../../data-types/string).

**القيمة المُعادة**

* تمثيل SVG للهندسة. [String](../../data-types/string).
  * دائرة SVG
  * مضلع SVG
  * مسار SVG

**أمثلة**

**دائرة**

```sql title="Query"
SELECT SVG((0., 0.))
```

```response title="Response"
<circle cx="0" cy="0" r="5" style=""/>
```

**Polygon**

```sql title="Query"
SELECT SVG([(0., 0.), (10, 0), (10, 10), (0, 10)])
```

```response title="Response"
<polygon points="0,0 0,10 10,10 10,0 0,0" style=""/>
```

**المسار**

```sql title="Query"
SELECT SVG([[(0., 0.), (10, 0), (10, 10), (0, 10)], [(4., 4.), (5, 4), (5, 5), (4, 5)]])
```

```response title="Response"
<g fill-rule="evenodd"><path d="M 0,0 L 0,10 L 10,10 L 10,0 L 0,0M 4,4 L 5,4 L 5,5 L 4,5 L 4,4 z " style=""/></g>
```