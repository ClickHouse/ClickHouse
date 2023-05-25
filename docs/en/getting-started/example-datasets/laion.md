# Laion-400M dataset

The dataset contains 400 million images with English text. For more information follow this [link](https://laion.ai/blog/laion-400-open-dataset/). Laion provides even larger datasets (e.g. [5 billion](https://laion.ai/blog/laion-5b/)). Working with them will be similar.

The dataset has prepared embeddings for texts and images. This will be used to demonstrate [Approximate nearest neighbor search indexes](../../engines/table-engines/mergetree-family/annindexes.md).

## Prepare data

Embeddings are stored in `.npy` files, so we have to read them with python and merge with other data.

Download data and process it with simple `download.sh` script:

```bash
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/img_emb/img_emb_${1}.npy
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/metadata/metadata_${1}.parquet
wget --tries=100 https://deploy.laion.ai/8f83b608504d46bb81708ec86e912220/embeddings/text_emb/text_emb_${1}.npy
python3 process.py ${1}
```

Where `process.py`:

```python
import pandas as pd
import numpy as np
import os
import sys

str_i = str(sys.argv[1])
npy_file = "img_emb_" + str_i + '.npy'
metadata_file = "metadata_" + str_i + '.parquet'
text_npy =  "text_emb_" + str_i + '.npy'

# load all files
im_emb = np.load(npy_file)
text_emb = np.load(text_npy) 
data = pd.read_parquet(metadata_file)

# combine them
data = pd.concat([data, pd.DataFrame({"image_embedding" : [*im_emb]}), pd.DataFrame({"text_embedding" : [*text_emb]})], axis=1, copy=False)

# you can save more columns
data = data[['url', 'caption', 'similarity', "image_embedding", "text_embedding"]]

# transform np.arrays to lists
data['image_embedding'] = data['image_embedding'].apply(lambda x: list(x))
data['text_embedding'] = data['text_embedding'].apply(lambda x: list(x))

# this small hack is needed becase caption sometimes contains all kind of quotes
data['caption'] = data['caption'].apply(lambda x: x.replace("'", " ").replace('"', " "))

# save data to file
data.to_csv(str_i + '.csv', header=False)

# previous files can be removed
os.system(f"rm {npy_file} {metadata_file} {text_npy}")
```

You can download data with
```bash
seq 0 409 | xargs -P100 -I{} bash -c './download.sh {}'
```

The dataset is divided into 409 files. If you want to work only with a certain part of the dataset, just change the limits.

## Create table for laion

Without indexes table can be created by

```sql
CREATE TABLE laion_dataset
(
    `id` Int64,
    `url` String,
    `caption` String,
    `similarity` Float32,
    `image_embedding` Array(Float32),
    `text_embedding` Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

Fill table with data:

```sql
INSERT INTO laion_dataset FROM INFILE '{path_to_csv_files}/*.csv'
```

## Check data in table without indexes

Let's check the work of the following query on the part of the dataset (8 million records):

```sql
select url, caption from test_laion where similarity > 0.2 order by L2Distance(image_embedding, {target:Array(Float32)}) limit 30
```

Since the embeddings for images and texts may not match, let's also require a certain threshold of matching accuracy to get images that are more likely to satisfy our queries. The client parameter `target`, which is an array of 512 elements. See later in this article for a convenient way of obtaining such vectors. I used a random picture of a cat from the Internet as a target vector.

**The result**

```
┌─url───────────────────────────────────────────────────────────────────────────────────────────────────────────┬─caption────────────────────────────────────────────────────────────────┐
│ https://s3.amazonaws.com/filestore.rescuegroups.org/6685/pictures/animals/13884/13884995/63318230_463x463.jpg │ Adoptable Female Domestic Short Hair                                   │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/8/b/6/239905226.jpg                                        │ Adopt A Pet :: Marzipan - New York, NY                                 │
│ http://d1n3ar4lqtlydb.cloudfront.net/9/2/4/248407625.jpg                                                      │ Adopt A Pet :: Butterscotch - New Castle, DE                           │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/e/e/c/245615237.jpg                                        │ Adopt A Pet :: Tiggy - Chicago, IL                                     │
│ http://pawsofcoronado.org/wp-content/uploads/2012/12/rsz_pumpkin.jpg                                          │ Pumpkin an orange tabby  kitten for adoption                           │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/7/8/3/188700997.jpg                                        │ Adopt A Pet :: Brian the Brad Pitt of cats - Frankfort, IL             │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/8/b/d/191533561.jpg                                        │ Domestic Shorthair Cat for adoption in Mesa, Arizona - Charlie         │
│ https://s3.amazonaws.com/pet-uploads.adoptapet.com/0/1/2/221698235.jpg                                        │ Domestic Shorthair Cat for adoption in Marietta, Ohio - Daisy (Spayed) │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────┘

8 rows in set. Elapsed: 6.432 sec. Processed 19.65 million rows, 43.96 GB (3.06 million rows/s., 6.84 GB/s.)
```

## Add indexes

Create a new table or follow instructions from [alter documentation](../../sql-reference/statements/alter/skipping-index.md).

```sql
CREATE TABLE laion_dataset
(
    `id` Int64,
    `url` String,
    `caption` String,
    `similarity` Float32,
    `image_embedding` Array(Float32),
    `text_embedding` Array(Float32),
    INDEX annoy_image image_embedding TYPE annoy(1000) GRANULARITY 1000,
    INDEX annoy_text text_embedding TYPE annoy(1000) GRANULARITY 1000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

When created, the index will be built by L2Distance. You can read more about the parameters in the [annoy documentation](../../engines/table-engines/mergetree-family/annindexes.md#annoy-annoy). It makes sense to build indexes for a large number of granules. If you need good speed, then GRANULARITY should be several times larger than the expected number of results in the search. 
Now let's check again with the same query:

```sql
select url, caption from test_indexes_laion where similarity > 0.2 order by L2Distance(image_embedding, {target:Array(Float32)}) limit 8
```

**Result**

```
┌─url──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─caption──────────────────────────────────────────────────────────────┐
│ http://tse1.mm.bing.net/th?id=OIP.R1CUoYp_4hbeFSHBaaB5-gHaFj                                                                                                                         │ bed bugs and pets can cats carry bed bugs pets adviser               │
│ http://pet-uploads.adoptapet.com/1/9/c/1963194.jpg?336w                                                                                                                              │ Domestic Longhair Cat for adoption in Quincy, Massachusetts - Ashley │
│ https://thumbs.dreamstime.com/t/cat-bed-12591021.jpg                                                                                                                                 │ Cat on bed Stock Image                                               │
│ https://us.123rf.com/450wm/penta/penta1105/penta110500004/9658511-portrait-of-british-short-hair-kitten-lieing-at-sofa-on-sun.jpg                                                    │ Portrait of british short hair kitten lieing at sofa on sun.         │
│ https://www.easypetmd.com/sites/default/files/Wirehaired%20Vizsla%20(2).jpg                                                                                                          │ Vizsla (Wirehaired) image 3                                          │
│ https://images.ctfassets.net/yixw23k2v6vo/0000000200009b8800000000/7950f4e1c1db335ef91bb2bc34428de9/dog-cat-flickr-Impatience_1.jpg?w=600&h=400&fm=jpg&fit=thumb&q=65&fl=progressive │ dog and cat image                                                    │
│ https://i1.wallbox.ru/wallpapers/small/201523/eaa582ee76a31fd.jpg                                                                                                                    │ cats, kittens, faces, tonkinese                                      │
│ https://www.baxterboo.com/images/breeds/medium/cairn-terrier.jpg                                                                                                                     │ Cairn Terrier Photo                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────┘

8 rows in set. Elapsed: 0.641 sec. Processed 22.06 thousand rows, 49.36 MB (91.53 thousand rows/s., 204.81 MB/s.)
```

The speed has increased significantly. But now, the results sometimes differ from what you are looking for. This is due to the approximation of the search and the quality of the constructed embedding. Note that the example was given for picture embeddings, but there are also text embeddings in the dataset, which can also be used for searching.

## Scripts for embeddings

Usually, we do not want to get embeddings from existing data, but to get them for new data and look for similar ones in old data. We can use [UDF](../../sql-reference/functions/index.md#sql-user-defined-functions) for this purpose. They will allow you to set the `target` vector without leaving the client. All of the following scripts will be written for the `ViT-B/32` model, as it was used for this dataset. You can use any model, but it is necessary to build embeddings in the dataset and for new objects using the same model.

### Text embeddings

`encode_text.py`:
```python
#!/usr/bin/python3
import clip
import torch
import numpy as np
import sys

if __name__ == '__main__':
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load("ViT-B/32", device=device)
    for text in sys.stdin:
        inputs = clip.tokenize(text)
        with torch.no_grad():
            text_features = model.encode_text(inputs)[0].tolist()
        sys.stdout.flush()
```

`encode_text_function.xml`:
```xml
<functions>
    <function>
        <type>executable</type>
        <name>encode_text</name>
        <return_type>Array(Float32)</return_type>
        <argument>
            <type>String</type>
            <name>text</name>
        </argument>
        <format>TabSeparated</format>
        <command>encode_text.py</command>
        <command_read_timeout>1000000</command_read_timeout>
    </function>
</functions>
```

Now we can simply use:

```sql
SELECT encode_text('cat');
```

The first use will be slow because the model needs to be loaded. But repeated queries will be fast. Then we copy the results to ``set param_target=...`` and can easily write queries 

### Image embeddings

For pictures, the process is similar, but you send the path instead of the picture (if necessary, you can implement a download picture with processing, but it will take longer)

`encode_picture.py`
```python
#!/usr/bin/python3
import clip
import torch
import numpy as np
from PIL import Image
import sys

if __name__ == '__main__':
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model, preprocess = clip.load("ViT-B/32", device=device)
    for text in sys.stdin:
        image = preprocess(Image.open(text.strip())).unsqueeze(0).to(device)
        with torch.no_grad():
            image_features = model.encode_image(image)[0].tolist()
        print(image_features)
        sys.stdout.flush()
```

`encode_picture_function.xml`
```xml
<functions>
    <function>
        <type>executable_pool</type>
        <name>encode_picture</name>
        <return_type>Array(Float32)</return_type>
        <argument>
            <type>String</type>
            <name>path</name>
        </argument>
        <format>TabSeparated</format>
        <command>encode_picture.py</command>
        <command_read_timeout>1000000</command_read_timeout>
    </function>
</functions>
```

The query:
```sql
SELECT encode_picture('some/path/to/your/picture');
```
