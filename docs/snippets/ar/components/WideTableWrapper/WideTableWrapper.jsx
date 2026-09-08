const WideTableWrapper = ({ children }) => {
  const containerStyle = {
    overflow: "auto",
    maxWidth: "100%",
  };
  // يدعم Mintlify الأنماط المضمنة ويعرض عناصر MDX التابعة كما هي بدقة —
  // لذا يكفي هذا الغلاف البسيط لضمان سهولة قراءة المحتوى. أما تجربة المستخدم
  // الخاصة بشريط التمرير العلوي المتزامن في الإصدار الأصلي من Docusaurus
  // فهي ميزة إضافية يمكننا الاستغناء عنها من دون التأثير في المحتوى.
  return <div style={containerStyle}>{children}</div>;
};
export default WideTableWrapper;