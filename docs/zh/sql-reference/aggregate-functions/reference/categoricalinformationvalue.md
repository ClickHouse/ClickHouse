---
toc_priority: 250
---

# categoricalInformationValue {#categoricalinformationvalue}

对于每个类别计算 `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` 。

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

结果指示离散（分类）要素如何使用 `[category1, category2, ...]` 有助于使用学习模型预测`tag`的值。
