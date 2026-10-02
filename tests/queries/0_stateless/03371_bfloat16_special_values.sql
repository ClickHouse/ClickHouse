-- Check that Float32 and BFloat16 return the same values for calculations with special values
SELECT toFloat32(0.0) == toFloat32(-0.0),   toBFloat16(0.0) == toBFloat16(-0.0);
SELECT toFloat32(0.0) != toFloat32(-0.0),   toBFloat16(0.0) != toBFloat16(-0.0);
SELECT toFloat32(0.0) > toFloat32(-0.0),    toBFloat16(0.0) > toBFloat16(-0.0);
SELECT toFloat32(0.0) < toFloat32(-0.0),    toBFloat16(0.0) < toBFloat16(-0.0);
SELECT toFloat32(0.0) + toFloat32(-0.0),    toBFloat16(0.0) + toBFloat16(-0.0);
SELECT toFloat32(-0.0) + toFloat32(-0.0),   toBFloat16(-0.0) + toBFloat16(-0.0);
SELECT toFloat32(NaN) == toFloat32(NaN),    toBFloat16(NaN) == toBFloat16(NaN);
SELECT toFloat32(NaN) + toFloat32(NaN),     toBFloat16(NaN) + toBFloat16(NaN);
SELECT toFloat32(NaN) - toFloat32(NaN),     toBFloat16(NaN) - toBFloat16(NaN);
SELECT toFloat32(NaN) * toFloat32(NaN),     toBFloat16(NaN) * toBFloat16(NaN);
SELECT toFloat32(NaN) / toFloat32(NaN),     toBFloat16(NaN) / toBFloat16(NaN);
SELECT toFloat32(NaN) % toFloat32(NaN),     toBFloat16(NaN) % toBFloat16(NaN);
SELECT toFloat32(5.5) + toFloat32(NaN),     toBFloat16(5.5) + toBFloat16(NaN);
SELECT toFloat32(5.5) - toFloat32(NaN),     toBFloat16(5.5) - toBFloat16(NaN);
SELECT toFloat32(5.5) * toFloat32(NaN),     toBFloat16(5.5) * toBFloat16(NaN);
SELECT toFloat32(5.5) / toFloat32(NaN),     toBFloat16(5.5) / toBFloat16(NaN);
SELECT toFloat32(5.5) % toFloat32(NaN),     toBFloat16(5.5) % toBFloat16(NaN);
SELECT toFloat32(Inf) == toFloat32(Inf),    toBFloat16(Inf) == toBFloat16(Inf);
SELECT toFloat32(Inf) + toFloat32(Inf),     toBFloat16(Inf) + toBFloat16(Inf);
SELECT toFloat32(Inf) - toFloat32(Inf),     toBFloat16(Inf) - toBFloat16(Inf);
SELECT toFloat32(Inf) * toFloat32(Inf),     toBFloat16(Inf) * toBFloat16(Inf);
SELECT toFloat32(Inf) / toFloat32(Inf),     toBFloat16(Inf) / toBFloat16(Inf);
SELECT toFloat32(Inf) % toFloat32(Inf),     toBFloat16(Inf) % toBFloat16(Inf);
SELECT toFloat32(-Inf) == toFloat32(-Inf),  toBFloat16(-Inf) == toBFloat16(-Inf);
SELECT toFloat32(5.5) + toFloat32(Inf),     toBFloat16(5.5) + toBFloat16(Inf);
SELECT toFloat32(5.5) - toFloat32(Inf),     toBFloat16(5.5) - toBFloat16(Inf);
SELECT toFloat32(5.5) * toFloat32(Inf),     toBFloat16(5.5) * toBFloat16(Inf);
SELECT toFloat32(5.5) / toFloat32(Inf),     toBFloat16(5.5) / toBFloat16(Inf);
SELECT toFloat32(5.5) % toFloat32(Inf),     toBFloat16(5.5) % toBFloat16(Inf);

-- Test for Bug 77087
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (c0 Tuple(BFloat16)) ENGINE = SummingMergeTree() ORDER BY (c0) SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0; -- Disable sparse serialization, otherwise the test becomes flaky
INSERT INTO TABLE tab (c0) VALUES ((-0.0, )), ((nan, )), ((0.0, ));
SELECT c0 FROM tab FINAL;
DROP TABLE tab;

-- Test for Bug 77224
CREATE TABLE tab (c0 BFloat16 PRIMARY KEY) ENGINE = SummingMergeTree() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0; -- Disable sparse serialization, otherwise the test becomes flaky
INSERT INTO TABLE tab (c0) VALUES (nan), (-0.0);
INSERT INTO TABLE tab (c0) VALUES (0.0), (nan);
SELECT c0 FROM tab FINAL;
DROP TABLE tab;
