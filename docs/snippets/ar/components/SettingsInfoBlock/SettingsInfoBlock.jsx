const SettingsInfoBlock = ({ type, default_value, changeable_without_restart }) => {
  return (
    <div
      className="not-prose"
      style={{
        display: "flex",
        flexWrap: "wrap",
        alignItems: "baseline",
        columnGap: "0.5rem",
        rowGap: "0.125rem",
        margin: "0.375rem 0",
        fontSize: "0.8125rem",
        lineHeight: "1.125rem",
      }}
    >
      <div style={{ fontWeight: 600, opacity: 0.72 }}>النوع</div>
      <div style={{ overflowWrap: "anywhere" }}>{type}</div>
      <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>القيمة الافتراضية</div>
      <div style={{ overflowWrap: "anywhere" }}>{default_value}</div>
      {changeable_without_restart && (
        <div style={{ fontWeight: 600, opacity: 0.72, marginInlineStart: "0.5rem" }}>
          يمكن تغييره دون إعادة التشغيل
        </div>
      )}
      {changeable_without_restart && (
        <div style={{ overflowWrap: "anywhere" }}>
          {changeable_without_restart}
        </div>
      )}
    </div>
  );
};
export default SettingsInfoBlock;