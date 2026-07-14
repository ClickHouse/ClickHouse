export const Image = ({ img, alt, size, caption }) => {
  return (
    <Frame caption={caption}>
      <img src={img} alt={alt} />
    </Frame>
  );
};
